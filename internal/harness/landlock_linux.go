//go:build linux

package harness

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	landlockFailureExit = 125
	landlockVersionFlag = 1
	landlockPathBeneath = 1
	landlockMaxABI      = 5
)

const (
	landlockExecute uint64 = 1 << iota
	landlockWriteFile
	landlockReadFile
	landlockReadDir
	landlockRemoveDir
	landlockRemoveFile
	landlockMakeChar
	landlockMakeDir
	landlockMakeReg
	landlockMakeSock
	landlockMakeFIFO
	landlockMakeBlock
	landlockMakeSym
	landlockRefer
	landlockTruncate
	landlockIOCTLDev
)

type landlockRulesetAttr struct{ HandledAccessFS uint64 }

type landlockPathBeneathAttr struct {
	AllowedAccess uint64
	ParentFD      int32
}

type landlockCLI struct {
	probe     bool
	readOnly  []string
	readWrite []string
	command   []string
}

// RunLandlockLauncher confines the current process and execs args after "--".
// A caller embedding the library can expose this through a small helper binary;
// cmd/dsh also wires it to its private __landlock-run mode.
func RunLandlockLauncher(args []string, stdout, stderr io.Writer) int {
	runtime.LockOSThread()
	parsed, err := parseLandlockCLI(args)
	if err != nil {
		fmt.Fprintf(stderr, "landlock-run: usage error: %v\n", err)
		return landlockFailureExit
	}
	if parsed.probe {
		parsed.readOnly = []string{"/"}
	}
	abi, err := landlockRestrict(parsed.readOnly, parsed.readWrite)
	if err != nil {
		fmt.Fprintf(stderr, "landlock-run: %v\n", err)
		return landlockFailureExit
	}
	partial := abi < landlockMaxABI
	if parsed.probe {
		if partial {
			fmt.Fprintln(stdout, "landlock: partially enforced (older ABI)")
		} else {
			fmt.Fprintln(stdout, "landlock: fully enforced")
		}
		return 0
	}
	if partial {
		fmt.Fprintln(stderr, "landlock-run: partial enforcement (older Landlock ABI)")
	}
	program := parsed.command[0]
	if filepath, lookErr := exec.LookPath(program); lookErr == nil {
		program = filepath
	} else if !errors.Is(lookErr, exec.ErrNotFound) {
		fmt.Fprintf(stderr, "landlock-run: exec failed: %v\n", lookErr)
		return landlockFailureExit
	}
	if err := unix.Exec(program, parsed.command, os.Environ()); err != nil {
		fmt.Fprintf(stderr, "landlock-run: exec failed: %v\n", err)
		return landlockFailureExit
	}
	return 0
}

func parseLandlockCLI(args []string) (landlockCLI, error) {
	var parsed landlockCLI
	for index := 0; index < len(args); {
		switch args[index] {
		case "--probe":
			if len(args) != 1 {
				return landlockCLI{}, errors.New("--probe takes no other arguments")
			}
			parsed.probe = true
			index++
		case "--ro", "--rw":
			if index+1 >= len(args) {
				return landlockCLI{}, fmt.Errorf("%s requires a path", args[index])
			}
			if args[index] == "--ro" {
				parsed.readOnly = append(parsed.readOnly, args[index+1])
			} else {
				parsed.readWrite = append(parsed.readWrite, args[index+1])
			}
			index += 2
		case "--":
			parsed.command = append([]string(nil), args[index+1:]...)
			index = len(args)
		default:
			return landlockCLI{}, fmt.Errorf("unknown argument: %s", args[index])
		}
	}
	if !parsed.probe && len(parsed.command) == 0 {
		return landlockCLI{}, errors.New("missing `-- <argv>...` command")
	}
	return parsed, nil
}

func landlockRestrict(readOnly, readWrite []string) (int, error) {
	abiValue, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, landlockVersionFlag, 0, 0, 0)
	if errno != 0 {
		return 0, errors.New("landlock is not enforced by this kernel (ABI unsupported or disabled)")
	}
	abi := int(abiValue)
	handled := landlockFSMask(abi)
	attr := landlockRulesetAttr{HandledAccessFS: handled}
	ruleset, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0, 0, 0, 0)
	if errno != 0 {
		return 0, fmt.Errorf("landlock ruleset error: %w", errno)
	}
	fd := int(ruleset)
	defer unix.Close(fd)
	readAccess := (landlockExecute | landlockReadFile | landlockReadDir) & handled
	for _, path := range readOnly {
		if err := landlockAddRule(fd, path, readAccess); err != nil {
			return 0, err
		}
	}
	for _, path := range readWrite {
		if err := landlockAddRule(fd, path, handled); err != nil {
			return 0, err
		}
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return 0, fmt.Errorf("landlock ruleset error: %w", err)
	}
	if _, _, errno := unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, uintptr(fd), 0, 0); errno != 0 {
		return 0, fmt.Errorf("landlock ruleset error: %w", errno)
	}
	return abi, nil
}

func landlockFSMask(abi int) uint64 {
	mask := landlockRefer - 1
	if abi >= 2 {
		mask |= landlockRefer
	}
	if abi >= 3 {
		mask |= landlockTruncate
	}
	if abi >= 5 {
		mask |= landlockIOCTLDev
	}
	return mask
}

func landlockAddRule(ruleset int, path string, access uint64) error {
	pathFD, err := unix.Open(path, unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("cannot open rule path: %s: %w", path, err)
	}
	defer unix.Close(pathFD)
	var stat unix.Stat_t
	if err := unix.Fstat(pathFD, &stat); err == nil && stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		access &= landlockExecute | landlockWriteFile | landlockReadFile | landlockTruncate | landlockIOCTLDev
	}
	attr := landlockPathBeneathAttr{AllowedAccess: access, ParentFD: int32(pathFD)}
	if _, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, uintptr(ruleset), landlockPathBeneath, uintptr(unsafe.Pointer(&attr)), 0, 0, 0); errno != 0 {
		return fmt.Errorf("landlock ruleset error: %w", errno)
	}
	return nil
}
