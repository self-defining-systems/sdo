package runtime

import ()

func rv6Guard(c *Controller, root, compiled string) {
	c.CheckHealth = acceptedHealthGuard(root, compiled)
}
