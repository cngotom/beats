package collstats

import (
	"errors"
	"strings"

	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/libbeat/common"
)

func eventMappingDiff(key string, data common.MapStr, oldData common.MapStr) (common.MapStr, error) {
	names := strings.SplitN(key, ".", 2)

	if len(names) < 2 {
		return nil, errors.New("Collection name invalid")
	}

	event := common.MapStr{
		"db":         names[0],
		"collection": names[1],
		"name":       key,
		"total": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "total.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "total.count"),
		},
		"lock": common.MapStr{
			"read": common.MapStr{
				"time": common.MapStr{
					"us": mustDiffMapStrValue(oldData, data, "readLock.time"),
				},
				"count": mustDiffMapStrValue(oldData, data, "readLock.count"),
			},
			"write": common.MapStr{
				"time": common.MapStr{
					"us": mustDiffMapStrValue(oldData, data, "writeLock.time"),
				},
				"count": mustDiffMapStrValue(oldData, data, "writeLock.count"),
			},
		},
		"queries": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "queries.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "queries.count"),
		},
		"getmore": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "getmore.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "getmore.count"),
		},
		"insert": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "insert.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "insert.count"),
		},
		"update": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "update.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "update.count"),
		},
		"remove": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "remove.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "remove.count"),
		},
		"commands": common.MapStr{
			"time": common.MapStr{
				"us": mustDiffMapStrValue(oldData, data, "commands.time"),
			},
			"count": mustDiffMapStrValue(oldData, data, "commands.count"),
		},
	}

	return event, nil
}
func convertToInt(value interface{}) int {
	switch v := value.(type) {
	case float32:
		return int(v)

	case float64:
		// numeric in sqlite3 sends us float64

		return int(v)
	case int64:
		// at least in sqlite3 when the value is 0 in db, the data is sent
		// to us as an int64 instead of a float64 ...
		return int(v)
	case int:
		return int(v)
	default:
		logp.Warn("unknow type %v", v)

		return 0
	}

}

func castDiff(o, v interface{}) interface{} {

	return convertToInt(v) - convertToInt(o)
	//return dv.Add(do.Neg())
}

func mustDiffMapStrValue(oldData common.MapStr, m common.MapStr, key string) interface{} {
	o, err := oldData.GetValue(key)
	if err != nil {
		o = 0
	}
	v, _ := m.GetValue(key)

	res := castDiff(o, v)
	//	logp.Info("Old %d, v %d, key %s, diff %s", o, v, key, res)
	return res
}
func mustGetMapStrValue(m common.MapStr, key string) interface{} {
	v, _ := m.GetValue(key)
	return v
}
