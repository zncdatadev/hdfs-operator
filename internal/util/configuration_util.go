package util

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
)

type NameValuePair struct {
	Name  string
	Value string
}

// MakeConfigFileContent returns the content of a configuration file
// content such as:
// ```
// key1 value1
// key2 value2
// ```
func MakeConfigFileContent(config map[string]string) string {
	content := ""
	if len(config) == 0 {
		return content
	}
	for k, v := range config {
		content += fmt.Sprintf("%s %s\n", k, v)
	}
	return content
}

// MakePropertiesFileContent returns the content of a properties file
// content such as:
// ```properties
// key1=value1
// key2=value2
// ```
func MakePropertiesFileContent(config map[string]string) string {
	content := ""
	if len(config) == 0 {
		return content
	}
	for k, v := range config {
		content += fmt.Sprintf("%s=%s\n", k, v)
	}
	return content
}

func OverrideConfigFileContent(current string, override string) string {
	if current == "" {
		return override
	}
	if override == "" {
		return current
	}
	return current + "\n" + override
}

// OverridePropertiesFileContent use bufio resolve properties
func OverridePropertiesFileContent(current string, override map[string]string) (string, error) {
	var properties []NameValuePair
	// scan current
	if err := ScanProperties(current, &properties); err != nil {
		logger.Error(err, "failed to scan current properties")
		return "", err
	}
	// override
	OverrideProperties(override, &properties)

	// to string
	var res string
	for _, v := range properties {
		res += fmt.Sprintf("%s=%s\n", v.Name, v.Value)
	}
	return res, nil
}

func ScanProperties(current string, properties *[]NameValuePair) error {
	scanner := bufio.NewScanner(strings.NewReader(current))

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || len(line) == 0 {
			continue
		}

		items := strings.Split(line, "=")
		if len(items) == 2 {
			*properties = append(*properties, NameValuePair{
				Name:  items[0],
				Value: items[1],
			})
		} else {
			return fmt.Errorf("invalid property line: %s", line)
		}
	}
	return scanner.Err()
}

// AppendXmlContent overrides the content of a xml file
// append the override properties to the current xml dom
func AppendXmlContent(current string, overrideProperties map[string]string) string {
	var xmlDom XmlConfiguration
	// string -> dom
	if err := xml.Unmarshal([]byte(current), &xmlDom); err != nil {
		panic(err)
	}
	// do override
	for k, v := range overrideProperties {
		overridePair := XmlNameValuePair{
			Name:  k,
			Value: v,
		}
		xmlDom.Properties = append(xmlDom.Properties, overridePair)
	}
	// dom -> string
	var b bytes.Buffer
	if _, err := b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"); err != nil {
		logger.Error(err, "failed to write string")
	}
	encoder := xml.NewEncoder(&b)
	encoder.Indent("", "  ")
	if err := encoder.Encode(xmlDom); err != nil {
		logger.Error(err, "failed to encode xml")
	}
	return b.String()
}

func OverrideProperties(override map[string]string, current *[]NameValuePair) {
	if len(override) == 0 {
		return
	}
	var currentKeys = make(map[string]int)
	for i, v := range *current {
		currentKeys[v.Name] = i
	}

	for k, v := range override {
		if _, ok := currentKeys[k]; ok {
			(*current)[currentKeys[k]].Value = v // override
		} else {
			// append new
			*current = append(*current, NameValuePair{
				Name:  k,
				Value: v,
			})
		}
	}
}
