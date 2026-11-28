package runtime

import (
	"errors"
	"time"

	"sdo.dev/operator/sdk"
)

// A departure the application stayed healthy with did not break it, so the
// automatic stage leaves it. What is remembered is the content itself, not
// that the object once departed: a later write can leave no drift event of its
// own (another manager already owns the field), and its content must qualify.

const absentContent = "absent"

// objectContent is the digest of what the source comparison sees of an object.
// Empty values are dropped so a typed and an unstructured read of one object
// agree. A gone object has its own value; an unreadable one, and a Secret, has
// none.
func objectContent(view any, ref sdk.ObjectRef) string {
	reader, ok := view.(sdk.ObjectReader)
	if !ok || ref.Kind == "Secret" {
		return ""
	}
	object, err := reader.Object(ref)
	switch {
	case errors.Is(err, sdk.ErrObjectAbsent), err == nil && object == nil:
		return absentContent
	case err != nil:
		return ""
	}
	return digestJSON(withoutEmpty(comparableFields(object.Object)))
}

func withoutEmpty(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		kept := map[string]any{}
		for key, child := range typed {
			if child = withoutEmpty(child); child != nil {
				kept[key] = child
			}
		}
		if len(kept) == 0 {
			return nil
		}
		return kept
	case []any:
		if len(typed) == 0 {
			return nil
		}
		items := make([]any, len(typed))
		for index, child := range typed {
			items[index] = withoutEmpty(child)
		}
		return items
	}
	return value
}

// seenHealthy reports that the object's content now is the content the event
// records as seen healthy.
func seenHealthy(view any, ref sdk.ObjectRef, event *sdk.DriftEvent) bool {
	return event.HealthyContent != "" && event.HealthyContentSince != nil && objectContent(view, ref) == event.HealthyContent
}

// observeSeenHealthy feeds one evaluation's health to the shared settle rule,
// so a fault still below its firing threshold does not count as healthy. When
// a stretch settles, each drifted object whose content was the same when the
// stretch began is recorded as seen healthy with it, on the events observed
// before the stretch.
func (c *Controller) observeSeenHealthy(now time.Time, snapshot sdk.DetectionContext, healthy bool) {
	c.mu.Lock()
	stretch, from := c.healthy.observe(now.UTC(), healthy)
	var refs []sdk.ObjectRef
	if stretch == stretchStarted || stretch == stretchSettled {
		for _, event := range c.driftEvents {
			refs = append(refs, event.Ref)
		}
	}
	c.mu.Unlock()
	if stretch != stretchStarted && stretch != stretchSettled {
		return
	}
	// Reads happen outside c.mu: an object missing from the snapshot is a GET.
	content := map[sdk.ObjectRef]string{}
	if len(refs) > 0 && c.drift != nil && c.drift.View != nil {
		view := c.drift.View(snapshot)
		for _, ref := range refs {
			if _, read := content[ref]; !read {
				content[ref] = objectContent(view, ref)
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stretch == stretchSettled {
		for index := range c.driftEvents {
			event := &c.driftEvents[index]
			if digest := content[event.Ref]; digest != "" && digest == c.stretchContent[event.Ref] && !event.ObservedAt.After(from) {
				since := from
				event.HealthyContent, event.HealthyContentSince = digest, &since
			}
		}
	}
	c.stretchContent = content
}
