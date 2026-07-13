package config

import "strings"

const DefaultModelProfileName = "default"

func (c Config) FindProfile(ref string) (Profile, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return Profile{}, false
	}
	for _, p := range c.Profiles {
		if p.ID == ref || strings.EqualFold(p.Name, ref) {
			return p, true
		}
	}
	return Profile{}, false
}

func (c *Config) UpsertProfile(p Profile) {
	for i := range c.Profiles {
		if c.Profiles[i].ID == p.ID {
			c.Profiles[i] = p
			return
		}
	}
	c.Profiles = append(c.Profiles, p)
}

func (c Config) InstancesForProfile(profileID string) []Instance {
	var out []Instance
	for _, inst := range c.Instances {
		if inst.ProfileID == profileID {
			out = append(out, inst)
		}
	}
	return out
}

func (c *Config) UpsertInstance(inst Instance) {
	for i := range c.Instances {
		if c.Instances[i].ID == inst.ID {
			c.Instances[i] = inst
			return
		}
	}
	c.Instances = append(c.Instances, inst)
}

func (c *Config) RemoveInstance(id string) bool {
	for i := range c.Instances {
		if c.Instances[i].ID != id {
			continue
		}
		c.Instances = append(c.Instances[:i], c.Instances[i+1:]...)
		return true
	}
	return false
}

func (c Config) EffectiveDefaultModelProfile() string {
	name := strings.TrimSpace(c.DefaultModelProfile)
	if name == "" {
		return DefaultModelProfileName
	}
	return name
}

// EffectiveDefaultModelSelector returns the canonical global model selector.
// The nested defaults container takes precedence over the legacy named-profile
// field. Consumers that only understand named profiles should continue using
// EffectiveDefaultModelProfile until they are migrated to selector-aware
// resolution.
func (c Config) EffectiveDefaultModelSelector() string {
	if c.Defaults != nil {
		if selector := strings.TrimSpace(c.Defaults.Model); selector != "" {
			return selector
		}
	}
	name := c.EffectiveDefaultModelProfile()
	if strings.EqualFold(name, DefaultModelProfileName) {
		return DefaultModelProfileName
	}
	return "profile:" + name
}

func (c Config) ExplicitDefaultReasoningEffort() string {
	if c.Defaults == nil {
		return ""
	}
	return strings.TrimSpace(c.Defaults.ReasoningEffort)
}

func (c Config) HasExplicitGlobalDefaults() bool {
	return c.Defaults != nil && (strings.TrimSpace(c.Defaults.Model) != "" || strings.TrimSpace(c.Defaults.ReasoningEffort) != "")
}

func (c *Config) EnsureGlobalDefaults() *GlobalDefaults {
	if c.Defaults == nil {
		c.Defaults = &GlobalDefaults{}
	}
	return c.Defaults
}

// SetDefaultModelProfile updates the legacy field and the selector-aware typed
// default together. Use this for named-profile/default mutations so older
// callers cannot leave the two representations disagreeing.
func (c *Config) SetDefaultModelProfile(name string) {
	previous := c.EffectiveDefaultModelSelector()
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, DefaultModelProfileName) {
		c.DefaultModelProfile = ""
		if c.Defaults != nil {
			c.Defaults.Model = ""
			if !strings.EqualFold(previous, DefaultModelProfileName) {
				c.Defaults.ReasoningEffort = ""
			}
			c.PruneEmptyGlobalDefaults()
		}
		return
	}
	c.DefaultModelProfile = name
	next := "profile:" + name
	defaults := c.EnsureGlobalDefaults()
	defaults.Model = next
	if !strings.EqualFold(previous, next) {
		// Legacy local commands cannot validate an arbitrary typed effort against
		// the new model. Clearing it is the conservative atomic fallback; the
		// Control-chat default manager performs richer preserve/reset validation.
		defaults.ReasoningEffort = ""
	}
}

func (c *Config) PruneEmptyGlobalDefaults() {
	if c.Defaults != nil && strings.TrimSpace(c.Defaults.Model) == "" && strings.TrimSpace(c.Defaults.ReasoningEffort) == "" {
		c.Defaults = nil
	}
}

func (c Config) FindModelProfile(ref string) (ModelProfile, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = c.EffectiveDefaultModelProfile()
	}
	if strings.EqualFold(ref, DefaultModelProfileName) {
		return ModelProfile{
			Provider: DefaultModelProfileName,
			Revision: 1,
		}, true
	}
	for name, profile := range c.ModelProfiles {
		if strings.EqualFold(strings.TrimSpace(name), ref) {
			return profile, true
		}
	}
	return ModelProfile{}, false
}

func (c *Config) UpsertModelProfile(name string, p ModelProfile) {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, DefaultModelProfileName) {
		return
	}
	if c.ModelProfiles == nil {
		c.ModelProfiles = map[string]ModelProfile{}
	}
	if p.Revision <= 0 {
		p.Revision = 1
	}
	c.ModelProfiles[name] = p
}

func (c *Config) RemoveModelProfile(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, DefaultModelProfileName) || c.ModelProfiles == nil {
		return false
	}
	for existing := range c.ModelProfiles {
		if strings.EqualFold(existing, name) {
			delete(c.ModelProfiles, existing)
			legacyWasEffective := c.Defaults == nil || strings.TrimSpace(c.Defaults.Model) == ""
			if strings.EqualFold(c.DefaultModelProfile, existing) {
				c.DefaultModelProfile = ""
				if legacyWasEffective && c.Defaults != nil {
					c.Defaults.ReasoningEffort = ""
				}
			}
			if c.Defaults != nil {
				selector := strings.TrimSpace(c.Defaults.Model)
				profileSelector := selector
				isProfileSelector := strings.HasPrefix(strings.ToLower(selector), "profile:")
				if isProfileSelector {
					_, profileSelector, _ = strings.Cut(selector, ":")
					profileSelector = strings.TrimSpace(profileSelector)
				}
				if strings.EqualFold(profileSelector, existing) && (isProfileSelector || strings.EqualFold(selector, existing)) {
					c.Defaults.Model = ""
					c.Defaults.ReasoningEffort = ""
					c.PruneEmptyGlobalDefaults()
				}
			}
			return true
		}
	}
	return false
}
