package v1alpha1

// HDFS image defaults. The image itself is modeled by the SDK commonsv1alpha1.ImageSpec
// (spec.image); these constants supply the product-specific default values that the operator
// (webhook defaulter / handler) applies when the user does not override them.
const (
	DefaultRepository      = "quay.io/zncdatadev"
	DefaultProductVersion  = "3.4.1"
	DefaultProductName     = "hadoop"
	DefaultKubedoopVersion = "0.0.0-dev"
)
