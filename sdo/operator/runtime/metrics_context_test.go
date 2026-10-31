package runtime

import (
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestMetricsCapabilitiesSurviveIncidentProjection(t *testing.T) {
	source := configuredMetricsSource("http://external:9090", "http://owned:9090")
	if !source.AnnotatedPodScraping || source.OtlpMetricsUrl != "http://owned:9090/api/v1/otlp/v1/metrics" {
		t.Fatal(source)
	}
	request := IncidentRequest{MetricsSource: source}
	message, err := incidentRequestContract(request)
	if err != nil {
		t.Fatal(err)
	}
	recovered := incidentRequestState(message)
	if !proto.Equal(recovered.MetricsSource, source) {
		t.Fatal("metrics source lost in projection")
	}
	cloned := cloneIncidentRequest(recovered)
	cloned.MetricsSource.QueryUrl = "changed"
	if recovered.MetricsSource.QueryUrl != source.QueryUrl {
		t.Fatal("clone aliases metrics source")
	}
	if configuredMetricsSource("http://external", "").AnnotatedPodScraping {
		t.Fatal("external URL implies scraping")
	}
	if configuredMetricsSource("", "").QueryUrl != "" {
		t.Fatal("absent source")
	}
}
