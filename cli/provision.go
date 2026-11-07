package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/internal/provisionhost"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
)

// NewCmdCamera embeds the same engine as the standalone host when --host-config
// is given, or submits asynchronous work to a configured provisioner otherwise.
func NewCmdCamera() *xli.Command {
	flags := func() flg.Flags {
		return flg.Flags{
			&flg.String{Name: "host-config", Brief: "private provisioner YAML configuration; run locally when supplied"},
			&flg.String{Name: "server", Brief: "provisioner HTTPS URL (remote commands)"},
			&flg.String{Name: "token-file", Brief: "private bearer token file for the provisioner"},
			&flg.String{Name: "ca-file", Brief: "additional CA certificate for the provisioner"},
			&flg.Switch{Name: "allow-loopback-http", Brief: "allow explicit HTTP to a loopback address for development"},
		}
	}
	requestFlags := func() flg.Flags {
		return append(flags(),
			&flg.String{Name: "request", Brief: "JSON enrollment request containing references, never passwords"},
			&flg.Switch{Name: "wait", Brief: "wait for an asynchronous job to finish or need input"},
		)
	}
	commands := []*xli.Command{}
	commands = append(commands, &xli.Command{Name: "credential", Brief: "prepare a recoverable credential and print only its scoped versioned reference", Flags: append(flags(),
		&flg.String{Name: "request", Brief: "JSON enrollment request supplying authorized scope and provider"},
		&flg.String{Name: "purpose", Brief: "initial_admin, management or streaming"},
		&flg.String{Name: "intent", Brief: "stable credential preparation key; initial_admin defaults to the enrollment's activation key"},
		&flg.String{Name: "material-file", Brief: "private JSON file with username/password; '-' reads stdin"},
	), Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
		path, _ := flg.Find[string](self, "host-config")
		if path == "" {
			return errors.New("--host-config is required")
		}
		config, err := provisionhost.ReadConfig(path)
		if err != nil {
			return err
		}
		request, err := cameraRequest(self)
		if err != nil {
			return err
		}
		authorized := false
		for _, target := range config.Targets {
			if target.Scope == request.Scope && target.Provider == request.Provider && target.Worker.URL == "" {
				authorized = true
			}
		}
		if !authorized {
			return camera.ErrForbidden
		}
		var provider credential.FileProvider
		for _, entry := range config.Providers {
			if entry.Name == request.Provider {
				provider = credential.FileProvider{Dir: entry.Directory, Name: entry.Name}
			}
		}
		if provider.Dir == "" {
			return camera.ErrForbidden
		}
		purposeText, _ := flg.Find[string](self, "purpose")
		purpose := camera.AuthPurpose(purposeText)
		if purpose != camera.AuthInitialAdmin && purpose != camera.AuthManagement && purpose != camera.AuthStreaming {
			return errors.New("--purpose must be initial_admin, management or streaming")
		}
		intent, _ := flg.Find[string](self, "intent")
		if intent == "" && purpose == camera.AuthInitialAdmin {
			intent = provision.Key(request.Scope.TenantID, request.BindingID, "initial_admin")
		}
		if intent == "" {
			return errors.New("--intent is required")
		}
		materialPath, _ := flg.Find[string](self, "material-file")
		if materialPath == "" {
			return errors.New("--material-file is required")
		}
		var input io.Reader = self
		if materialPath != "-" {
			info, err := os.Lstat(materialPath)
			if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
				return errors.New("credential material requires a private regular file")
			}
			file, err := os.Open(materialPath)
			if err != nil {
				return errors.New("cannot open credential material")
			}
			defer file.Close()
			input = file
		}
		var material struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		decoder := json.NewDecoder(io.LimitReader(input, 64<<10))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&material) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return errors.New("invalid credential material")
		}
		ref, err := provider.Prepare(ctx, request.Scope, purpose, intent, camera.CredentialMaterial{Username: material.Username, Password: material.Password})
		if err != nil {
			return errors.New("cannot prepare immutable credential; check provider scope and existing intent")
		}
		return printCameraJSON(self, ref)
	})})
	for _, name := range []string{"serve", "worker"} {
		name := name
		commands = append(commands, &xli.Command{Name: name, Brief: "serve camera provisioning with configured scopes and local or LAN workers", Flags: flags(), Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			path, _ := flg.Find[string](self, "host-config")
			if path == "" {
				return errors.New("--host-config is required")
			}
			config, err := provisionhost.ReadConfig(path)
			if err != nil {
				return err
			}
			if name == "worker" {
				for _, target := range config.Targets {
					if target.Worker.URL != "" {
						return errors.New("worker targets must be local")
					}
				}
			}
			built, err := provisionhost.Build(config)
			if err != nil {
				return err
			}
			defer built.Close()
			return built.Serve(ctx, nil)
		})})
	}
	for _, name := range []string{"inspect", "plan", "apply"} {
		name := name
		commands = append(commands, &xli.Command{Name: name, Brief: "inspect and plan a camera enrollment; apply explicitly performs the planned workflow", Flags: requestFlags(), Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			request, err := cameraRequest(self)
			if err != nil {
				return err
			}
			path, _ := flg.Find[string](self, "host-config")
			if path != "" {
				config, err := provisionhost.ReadConfig(path)
				if err != nil {
					return err
				}
				built, err := provisionhost.Build(config)
				if err != nil {
					return err
				}
				defer built.Close()
				run, err := built.Execute(ctx, request, name == "apply")
				if run.Request.ID != "" {
					if writeErr := printCameraJSON(self, run); writeErr != nil {
						return writeErr
					}
				}
				return err
			}
			connection := cameraConnection(self)
			var job provisionhost.Job
			if err = provisionhost.Call(ctx, connection, http.MethodPost, "/v1/runs", map[string]any{"request": request, "apply": name == "apply"}, &job); err != nil {
				return err
			}
			return cameraPrintJob(ctx, self, connection, job)
		})})
	}
	commands = append(commands, &xli.Command{Name: "discover", Brief: "discover candidates in one authorized network; no credentials or camera writes", Flags: append(flags(), &flg.String{Name: "network", Brief: "configured network ID"}, &flg.String{Name: "id", Brief: "stable asynchronous discovery job ID"}, &flg.Switch{Name: "wait", Brief: "wait for asynchronous discovery results"}), Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
		network, _ := flg.Find[string](self, "network")
		if network == "" {
			return errors.New("--network is required")
		}
		if path, _ := flg.Find[string](self, "host-config"); path != "" {
			config, err := provisionhost.ReadConfig(path)
			if err != nil {
				return err
			}
			built, err := provisionhost.Build(config)
			if err != nil {
				return err
			}
			defer built.Close()
			observations, err := built.Discover(ctx, network)
			if err != nil {
				return err
			}
			return printCameraJSON(self, observations)
		}
		id, _ := flg.Find[string](self, "id")
		if id == "" {
			return errors.New("--id is required for asynchronous discovery")
		}
		connection := cameraConnection(self)
		var job provisionhost.Job
		if err := provisionhost.Call(ctx, connection, http.MethodPost, "/v1/discover", map[string]string{"id": id, "network_id": network}, &job); err != nil {
			return err
		}
		return cameraPrintJob(ctx, self, connection, job)
	})})
	for _, name := range []string{"status", "resume", "cancel"} {
		name := name
		commandFlags := append(flags(), &flg.String{Name: "id", Brief: "job ID; omit for status to list visible jobs"}, &flg.Switch{Name: "apply", Brief: "on resume, explicitly apply a previously read-only plan"}, &flg.Switch{Name: "wait", Brief: "wait for resumed work to finish or need input"})
		if name == "status" {
			commandFlags = append(commandFlags, &flg.Switch{Name: "summary", Brief: "show preset, effective settings and warnings as text; JSON is the default"})
		}
		commands = append(commands, &xli.Command{Name: name, Brief: "observe, resume or cancel a durable provisioning job", Flags: commandFlags, Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			if path, _ := flg.Find[string](self, "host-config"); path != "" {
				return errors.New("status, resume and cancel use --server and --token-file")
			}
			id, _ := flg.Find[string](self, "id")
			if id == "" && name != "status" {
				return errors.New("--id is required")
			}
			connection := cameraConnection(self)
			path := "/v1/runs"
			if id != "" {
				path += "/" + url.PathEscape(id)
			}
			if name == "status" {
				if summary, _ := flg.Find[bool](self, "summary"); summary {
					var jobs []provisionhost.Job
					if id == "" {
						if err := provisionhost.Call(ctx, connection, http.MethodGet, path, nil, &jobs); err != nil {
							return err
						}
					} else {
						var job provisionhost.Job
						if err := provisionhost.Call(ctx, connection, http.MethodGet, path, nil, &job); err != nil {
							return err
						}
						jobs = append(jobs, job)
					}
					return printCameraSummary(self, jobs)
				}
				var value any
				if err := provisionhost.Call(ctx, connection, http.MethodGet, path, nil, &value); err != nil {
					return err
				}
				return printCameraJSON(self, value)
			}
			input := map[string]any{}
			if apply, _ := flg.Find[bool](self, "apply"); apply {
				input["apply"] = true
			}
			var job provisionhost.Job
			if err := provisionhost.Call(ctx, connection, http.MethodPost, path+"/"+name, input, &job); err != nil {
				return err
			}
			return cameraPrintJob(ctx, self, connection, job)
		})})
	}
	return &xli.Command{Name: "camera", Brief: "discover and provision IP cameras using scoped credentials and durable plans", Commands: commands, Handler: xli.RequireSubcommand()}
}

func printCameraSummary(w io.Writer, jobs []provisionhost.Job) error {
	for _, job := range jobs {
		stage := job.State
		if job.Run != nil {
			stage = string(job.Run.Stage)
		}
		if _, err := fmt.Fprintf(w, "%s: %s (job: %s)\n", cameraStatusLabel(job.ID), cameraStatusLabel(stage), cameraStatusLabel(job.State)); err != nil {
			return err
		}
		if job.Run == nil {
			continue
		}
		run := job.Run
		if run.Preset != nil {
			if _, err := fmt.Fprintf(w, "  preset: %q revision %q\n  requested: %s\n", run.Preset.Ref.ID, run.Preset.Ref.Revision, cameraSettingsSummary(run.Preset.Requested)); err != nil {
				return err
			}
			if len(run.Preset.MatchedRules) != 0 {
				if _, err := fmt.Fprintf(w, "  matched rules: %q\n", run.Preset.MatchedRules); err != nil {
					return err
				}
			}
		}
		if run.Effective != nil {
			if _, err := fmt.Fprintf(w, "  effective: %s\n", cameraSettingsSummary(*run.Effective)); err != nil {
				return err
			}
		}
		for _, warning := range run.Warnings {
			if _, err := fmt.Fprintf(w, "  WARNING %q: %q requested=%q applied=%q reason=%q\n", warning.Code, warning.Field, warning.Requested, warning.Applied, warning.Reason); err != nil {
				return err
			}
		}
	}
	return nil
}

func cameraStatusLabel(value string) string {
	if strings.IndexFunc(value, func(r rune) bool { return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) }) >= 0 {
		return strconv.Quote(value)
	}
	return value
}

func cameraSettingsSummary(settings camera.PresetSettings) string {
	parts := []string{}
	if settings.Initialization.PowerLineFrequency != 0 {
		parts = append(parts, fmt.Sprintf("frequency=%dHz", settings.Initialization.PowerLineFrequency))
	}
	v := settings.Video
	if v.ProfileToken != "" {
		parts = append(parts, fmt.Sprintf("profile=%q", v.ProfileToken))
	}
	if v.Codec != "" {
		parts = append(parts, fmt.Sprintf("codec=%q", v.Codec))
	}
	if v.Width != 0 || v.Height != 0 {
		parts = append(parts, fmt.Sprintf("size=%dx%d", v.Width, v.Height))
	}
	if v.FPS != 0 {
		parts = append(parts, fmt.Sprintf("fps=%g", v.FPS))
	}
	if v.Bitrate != 0 {
		parts = append(parts, fmt.Sprintf("bitrate=%d", v.Bitrate))
	}
	if v.GOP != 0 {
		parts = append(parts, fmt.Sprintf("gop=%d", v.GOP))
	}
	if len(parts) == 0 {
		return "preserve current settings"
	}
	return strings.Join(parts, " ")
}

func cameraRequest(self *xli.Command) (provision.Request, error) {
	var request provision.Request
	path, _ := flg.Find[string](self, "request")
	if path == "" {
		return request, errors.New("--request is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return request, errors.New("cannot open camera request")
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		return request, camera.ErrInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return request, camera.ErrInvalid
	}
	return request, request.Validate()
}

func cameraConnection(self *xli.Command) provisionhost.ConnectionConfig {
	var config provisionhost.ConnectionConfig
	config.URL, _ = flg.Find[string](self, "server")
	config.TokenFile, _ = flg.Find[string](self, "token-file")
	config.CAFile, _ = flg.Find[string](self, "ca-file")
	config.AllowLoopbackHTTP, _ = flg.Find[bool](self, "allow-loopback-http")
	return config
}

func printCameraJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func cameraPrintJob(ctx context.Context, self *xli.Command, connection provisionhost.ConnectionConfig, job provisionhost.Job) error {
	wait, _ := flg.Find[bool](self, "wait")
	if wait {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for job.State == "queued" || job.State == "running" {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			if err := provisionhost.Call(ctx, connection, http.MethodGet, "/v1/runs/"+url.PathEscape(job.ID), nil, &job); err != nil {
				return err
			}
		}
	}
	return printCameraJSON(self, job)
}
