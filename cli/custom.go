package cli

import (
	"strings"

	"github.com/lesomnus/payday/pdcmd"

	"github.com/lesomnus/shale/cmd"
)

// custom are the operations that mean something (§32), mounted beside the
// generated verbs: each takes a REF where the request names a row, and the
// rest of the request as protojson.
var custom = map[string]string{
	"node/adopt":                "shale.NodeService.Adopt",
	"node/resolve":              "shale.NodeService.Resolve",
	"relay/adopt":               "shale.RelayService.Adopt",
	"relay/assign":              "shale.RelayService.Assign",
	"producer/adopt":            "shale.ProducerService.Adopt",
	"reader/adopt":              "shale.ReaderService.Adopt",
	"set/negotiate":             "shale.SetService.Negotiate",
	"set/allocate":              "shale.SetService.Allocate",
	"set/live":                  "shale.SetService.Live",
	"source/live":               "shale.SourceService.Live",
	"object/allocate":           "shale.ObjectService.Allocate",
	"object/reallocate":         "shale.ObjectService.Reallocate",
	"object/renew":              "shale.ObjectService.Renew",
	"object/report-attempt":     "shale.ObjectService.ReportAttempt",
	"object/report-failure":     "shale.ObjectService.ReportFailure",
	"object/reschedule":         "shale.ObjectService.Reschedule",
	"object/timeline":           "shale.ObjectService.Timeline",
	"sink/adopt":                "shale.SinkService.Adopt",
	"sink/retire":               "shale.SinkService.Retire",
	"device/quarantine":         "shale.DeviceService.Quarantine",
	"device/release":            "shale.DeviceService.Release",
	"device/retire":             "shale.DeviceService.Retire",
	"device/declare-dead":       "shale.DeviceService.DeclareDead",
	"device/locate":             "shale.DeviceService.Locate",
	"signing-key/rotate":        "shale.SigningKeyService.Rotate",
	"placement-policy/activate": "shale.PlacementPolicyService.Activate",
	"upload-policy/activate":    "shale.UploadPolicyService.Activate",
	"address-policy/activate":   "shale.AddressPolicyService.Activate",
}

func addCustom(t *pdcmd.Tree, c *cmd.Config, cluster bool) {
	for path, method := range custom {
		group := path[:strings.IndexByte(path, '/')]
		if clusterGroups[group] != cluster {
			continue
		}
		u, err := t.Unary(method)
		if err != nil {
			continue
		}
		t.Add(path, u)
	}
}
