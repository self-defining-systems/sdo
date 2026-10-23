package generators

import "sdo.dev/operator/sdk/traffic"

// The nginx-thrift gateway (Service nginx-thrift, port 8080) serves the static
// pages and the Lua handlers its nginx.conf maps. Timeline reads are the
// read-only journeys: the bundled wrk2 load generator reads user ids in
// [0, max_user_index) with max_user_index defaulting to 962 and a window of
// ten posts. A timeline is a JSON array of post objects; lua-cjson encodes an
// empty timeline as "{}", so every healthy body contains "{".
var timelineQuery = traffic.Params{
	"user_id": traffic.IntBetween(0, 961),
	"start":   traffic.Const("0"),
	"stop":    traffic.Const("10"),
}

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		{
			ID:          "landing-page",
			Description: "Load the landing page nginx-thrift serves from its pages directory; it carries the login form.",
			Target:      traffic.Target{Service: "nginx-thrift", Port: 8080},
			DependsOn:   []string{"nginx-thrift"},
			SideEffect:  traffic.SideEffectRead,
			Steps:       []traffic.Step{{Name: "index", Endpoint: traffic.GET("/", nil).Contains("api/user/login")}},
			Detects:     []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "home-timeline-read",
			Description: "Read the newest home-timeline posts of a user through home-timeline-service and post-storage-service.",
			Target:      traffic.Target{Service: "nginx-thrift", Port: 8080},
			DependsOn:   []string{"nginx-thrift", "home-timeline-service", "home-timeline-redis", "post-storage-service", "post-storage-memcached", "post-storage-mongodb"},
			SideEffect:  traffic.SideEffectRead,
			Steps:       []traffic.Step{{Name: "read", Endpoint: traffic.GET("/wrk2-api/home-timeline/read", timelineQuery).Contains("{")}},
			Detects:     []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "user-timeline-read",
			Description: "Read the newest posts a user composed through user-timeline-service and post-storage-service.",
			Target:      traffic.Target{Service: "nginx-thrift", Port: 8080},
			DependsOn:   []string{"nginx-thrift", "user-timeline-service", "user-timeline-redis", "user-timeline-mongodb", "post-storage-service", "post-storage-memcached", "post-storage-mongodb"},
			SideEffect:  traffic.SideEffectRead,
			Steps:       []traffic.Step{{Name: "read", Endpoint: traffic.GET("/wrk2-api/user-timeline/read", timelineQuery).Contains("{")}},
			Detects:     []traffic.FaultClass{traffic.FaultWrongBody},
		},
	}
}
