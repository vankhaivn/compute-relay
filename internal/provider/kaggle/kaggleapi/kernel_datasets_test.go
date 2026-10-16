package kaggleapi

import (
	"strconv"
	"testing"
)

func TestKernelMountsOnlyBoundedDistinctOwnedDatasets(t *testing.T) {
	own := func(slug string) Resource { return Resource{"owner", slug} }
	many := make([]Resource, 0, maxKernelDatasets+1)
	for i := 0; i <= maxKernelDatasets; i++ {
		many = append(many, own("crx-"+strconv.Itoa(i)))
	}
	cases := map[string]struct {
		datasets []Resource
		ok       bool
	}{
		"attempt and shared": {[]Resource{own("crs-a"), own("crx-b")}, true},
		"limit":              {many[:maxKernelDatasets], true},
		"none":               {nil, false},
		"over limit":         {many, false},
		"duplicate":          {[]Resource{own("crs-a"), own("crs-a")}, false},
		"foreign owner":      {[]Resource{own("crs-a"), {"other", "crx-b"}}, false},
		"invalid slug":       {[]Resource{own("crs-a"), own("../b")}, false},
	}
	for name, c := range cases {
		if ownedDatasets(c.datasets, "owner") != c.ok {
			t.Fatal("dataset mount rule failed", name)
		}
	}
}
