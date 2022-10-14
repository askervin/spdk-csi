/*
Copyright (c) Intel Corporation and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"encoding/json"
	"fmt"
)

//nolint:tagliatelle // not using json:snake case
type GlobalConfig struct {
	Nodes []struct {
		Name       string `json:"name"`
		URL        string `json:"rpcURL"`
		TargetType string `json:"targetType"`
		TargetAddr string `json:"targetAddr"`
	} `json:"Nodes"`
	Sma *SmaConfig `json:"sma"`
}

//nolint:tagliatelle // not using json:snake case
type SmaConfig struct {
	Server      string         `json:"server"`
	ClassConfig SmaClassConfig `json:"classConfig"`
}

//nolint:tagliatelle // not using json:snake case
type SmaClassConfig struct {
	Qos map[string]string `json:"qos"`
}

func (sc *SmaConfig) JSONString() string {
	b, err := json.Marshal(sc)
	if err != nil {
		return fmt.Sprintf("SMA Config marshal error: %v from config: %+v", err, sc)
	}
	return string(b)
}

func (scc *SmaClassConfig) JSONString() string {
	b, err := json.Marshal(scc)
	if err != nil {
		return fmt.Sprintf("SMA Class Config marshal error: %v from config: %+v", err, scc)
	}
	return string(b)
}
