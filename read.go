package testeval

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lincaiyong/larkbase"
	"strconv"
)

func SimpleReadFn(tableUrl string, testFields, evalFields []string) func(context.Context) ([]*Result, error) {
	return func(ctx context.Context) ([]*Result, error) {
		conn, err := larkbase.ConnectAny(ctx, appId, appSecret, tableUrl)
		if err != nil {
			return nil, err
		}
		var records []*larkbase.AnyRecord
		err = conn.FindAll(&records, larkbase.NewViewIdFindOption(conn.ViewId()))
		if err != nil {
			return nil, err
		}
		ret := make([]*Result, 0, len(records))
		for _, record := range records {
			sampleId, _ := strconv.Atoi(record.Data["id"])
			if sampleId == 0 {
				return nil, fmt.Errorf("id field is invalid from sample id: %s", record.Data)
			}
			data := make(map[string]string)
			for _, k := range testFields {
				if record.Data[k] != "" {
					data[k] = record.Data[k]
				}
			}
			b, _ := json.Marshal(data)
			testInput := string(b)
			data = make(map[string]string)
			for _, k := range evalFields {
				if record.Data[k] != "" {
					data[k] = record.Data[k]
				}
			}
			b, _ = json.Marshal(data)
			evalInput := string(b)
			result := NewResult(record.RecordId, sampleId, testInput, evalInput, "", "")
			ret = append(ret, result)
		}
		return ret, nil
	}
}
