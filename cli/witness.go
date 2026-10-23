package cli

// The witnesses `audit.checkpoints.witnesses` can name (§26.5): where the
// newest checkpoint of the trail's archive is heard outside the database and
// the archive. Blank-imported here, beside the database engines and for
// their reason: a process hears checkpoints, and the sandbox does not.
//
// `log:` tells the log and the metrics, which monitoring outside alerts on;
// `store:<dir>` keeps what it hears on a disk of its own, best one that
// refuses deletion and is mounted with credentials the database's and the
// archive's are not. A scheme this binary does not open is refused where
// the process comes up.
import (
	_ "github.com/lesomnus/payday/config/witnesslog"
	_ "github.com/lesomnus/payday/config/witnessstore"
)
