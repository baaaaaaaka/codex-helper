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
			if strings.EqualFold(c.DefaultModelProfile, existing) {
				c.DefaultModelProfile = ""
			}
			return true
		}
	}
	return false
}
