package util

import (
	"bytes"
	"encoding/xml"
)

type XmlNameValuePair struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type XmlConfiguration struct {
	XMLName    xml.Name           `xml:"configuration"`
	Properties []XmlNameValuePair `xml:"property"`
}

func NewXmlConfiguration(properties []XmlNameValuePair) *XmlConfiguration {
	return &XmlConfiguration{
		Properties: properties,
	}
}

func (c *XmlConfiguration) String(properties []XmlNameValuePair) string {
	if len(c.Properties) != 0 {
		c.Properties = append(c.Properties, properties...)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"); err != nil {
		logger.Error(err, "failed to write xml document head")
	}
	enc := xml.NewEncoder(buf)
	enc.Indent("", "  ")
	if err := enc.Encode(c); err != nil {
		logger.Error(err, "failed to encode xml document")
		panic(err)
	}
	return buf.String()
}
