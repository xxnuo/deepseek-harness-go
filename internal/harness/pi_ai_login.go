package harness

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

func (e *Engine) registerPiAIAuthFlows() error {
	routes := make([]string, 0, len(piAICatalog))
	for route := range piAICatalog {
		routes = append(routes, route)
	}
	sort.Strings(routes)
	for _, route := range routes {
		catalog := piAICatalog[route]
		key, ok := piAIRecordKey(route)
		if !ok {
			continue
		}
		methods := make([]AuthorizationMethod, 0, 2)
		if oauth := catalog.Auth.OAuth; oauth != nil && oauth.Login {
			label := firstNonBlank(oauth.LoginLabel, oauth.Name)
			methods = append(methods, AuthorizationMethod{ID: "oauth", Label: label})
		}
		if apiKey := catalog.Auth.APIKey; apiKey != nil && apiKey.Login {
			methods = append(methods, AuthorizationMethod{ID: "api-key", Label: apiKey.Name})
		}
		if len(methods) == 0 {
			continue
		}
		route, catalog := route, catalog
		if _, err := e.Authorization().RegisterFlow(AuthorizationFlow{
			Key: key, Label: catalog.DisplayName, Methods: methods,
			Run: func(session AuthorizationSession) error {
				var record CredentialRecord
				var err error
				switch session.Method {
				case "api-key":
					record, err = e.loginPiAIAPIKey(session, route, catalog)
				case "oauth":
					var payload map[string]any
					payload, err = e.loginPiAIOAuth(session, route)
					record = CredentialRecord{Kind: CredentialRecordGrant, Payload: payload}
				default:
					err = fmt.Errorf("unsupported pi-ai login method %q", session.Method)
				}
				if err != nil {
					return err
				}
				_, err = e.Credentials().ModifyRecord(session.Context, key, func(context.Context, *CredentialRecord) (*CredentialRecord, error) {
					return &record, nil
				})
				return err
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) loginPiAIAPIKey(session AuthorizationSession, route string, catalog piAICatalogRoute) (CredentialRecord, error) {
	prompt := func(kind AuthorizationPromptKind, message string, options ...AuthorizationPromptOption) (string, error) {
		answer, err := session.Prompt(AuthorizationPrompt{Kind: kind, Message: message, Options: options})
		return strings.TrimSpace(answer), err
	}
	switch route {
	case "amazon-bedrock":
		method, err := prompt(AuthorizationPromptSelect, "Select Amazon Bedrock authentication method:",
			AuthorizationPromptOption{ID: "bearer-token", Label: "Bearer token"},
			AuthorizationPromptOption{ID: "aws-profile", Label: "AWS profile"},
			AuthorizationPromptOption{ID: "credential-chain", Label: "Existing AWS credential chain"},
		)
		if err != nil {
			return CredentialRecord{}, err
		}
		switch method {
		case "bearer-token":
			key, err := prompt(AuthorizationPromptSecret, "Enter Amazon Bedrock bearer token")
			return CredentialRecord{Kind: CredentialRecordAPIKey, Key: key}, err
		case "aws-profile":
			profile, err := prompt(AuthorizationPromptText, "Enter AWS profile name")
			return CredentialRecord{Kind: CredentialRecordAPIKey, Env: map[string]string{"AWS_PROFILE": profile}}, err
		case "credential-chain":
			session.Notify(AuthorizationNotice{Message: "Amazon Bedrock supports AWS profiles, IAM credentials, and role-based credentials.", URL: "https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html"})
			_, err := prompt(AuthorizationPromptText, "Configure AWS credentials, then press Enter to continue")
			return CredentialRecord{Kind: CredentialRecordAPIKey}, err
		default:
			return CredentialRecord{}, fmt.Errorf("unknown Amazon Bedrock auth method: %s", method)
		}
	case "google-vertex":
		method, err := prompt(AuthorizationPromptSelect, "Select Google Vertex AI authentication method:",
			AuthorizationPromptOption{ID: "api-key", Label: "Google Cloud API key"},
			AuthorizationPromptOption{ID: "adc", Label: "Application Default Credentials"},
			AuthorizationPromptOption{ID: "service-account", Label: "Service account credentials file"},
		)
		if err != nil {
			return CredentialRecord{}, err
		}
		if method == "api-key" {
			key, err := prompt(AuthorizationPromptSecret, "Enter Google Cloud API key")
			return CredentialRecord{Kind: CredentialRecordAPIKey, Key: key}, err
		}
		if method != "adc" && method != "service-account" {
			return CredentialRecord{}, fmt.Errorf("unknown Google Vertex AI auth method: %s", method)
		}
		message := "Run `gcloud auth application-default login`, then provide the project and location."
		if method == "service-account" {
			message = "Provide a service account credentials file, project, and location."
		}
		session.Notify(AuthorizationNotice{Message: message, URL: "https://cloud.google.com/docs/authentication/provide-credentials-adc"})
		project, err := prompt(AuthorizationPromptText, "Enter Google Cloud project ID")
		if err != nil {
			return CredentialRecord{}, err
		}
		location, err := prompt(AuthorizationPromptText, "Enter Google Cloud location")
		if err != nil {
			return CredentialRecord{}, err
		}
		env := map[string]string{"GOOGLE_CLOUD_PROJECT": project, "GOOGLE_CLOUD_LOCATION": location}
		if method == "service-account" {
			path, err := prompt(AuthorizationPromptText, "Enter service account credentials file path")
			if err != nil {
				return CredentialRecord{}, err
			}
			env["GOOGLE_APPLICATION_CREDENTIALS"] = path
		}
		return CredentialRecord{Kind: CredentialRecordAPIKey, Env: env}, nil
	case "cloudflare-workers-ai", "cloudflare-ai-gateway":
		key, err := prompt(AuthorizationPromptSecret, "Enter Cloudflare API key")
		if err != nil {
			return CredentialRecord{}, err
		}
		account, err := prompt(AuthorizationPromptText, "Enter Cloudflare account ID")
		if err != nil {
			return CredentialRecord{}, err
		}
		env := map[string]string{"CLOUDFLARE_ACCOUNT_ID": account}
		if route == "cloudflare-ai-gateway" {
			gateway, err := prompt(AuthorizationPromptText, "Enter Cloudflare AI Gateway ID")
			if err != nil {
				return CredentialRecord{}, err
			}
			env["CLOUDFLARE_GATEWAY_ID"] = gateway
		}
		return CredentialRecord{Kind: CredentialRecordAPIKey, Key: key, Env: env}, nil
	default:
		name := "API key"
		if catalog.Auth.APIKey != nil {
			name = catalog.Auth.APIKey.Name
		}
		key, err := prompt(AuthorizationPromptSecret, "Enter "+name)
		return CredentialRecord{Kind: CredentialRecordAPIKey, Key: key}, err
	}
}
