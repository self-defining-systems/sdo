// Project the production worker mount policy for a read-only benchmark smoke.
package main

import (
 "encoding/json"
 "os"
 "strings"

 corev1 "k8s.io/api/core/v1"
 "sdo.dev/operator/devmount"
)

type sourceMount struct {
 Source string `json:"source"`
 Target string `json:"target"`
}

func main() {
 pod := corev1.PodSpec{Containers: []corev1.Container{{Name: "smoke"}}}
 devmount.ApplyWorker(&pod)
 sources := map[string]string{}
 for _, volume := range pod.Volumes {
  // The Python import probe never executes the mounted Go binary.
  if volume.Name != "dev-operator" {
   sources[volume.Name] = strings.TrimPrefix(volume.HostPath.Path, devmount.NodePath+"/")
  }
 }
 mounts := []sourceMount{}
 for _, mount := range pod.Containers[0].VolumeMounts {
  if source, ok := sources[mount.Name]; ok {
   mounts = append(mounts, sourceMount{Source: source, Target: mount.MountPath})
  }
 }
 if err := json.NewEncoder(os.Stdout).Encode(mounts); err != nil { panic(err) }
}
