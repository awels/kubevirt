/*
Copyright 2024 The KubeVirt Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package v1beta1

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
	scheme "kubevirt.io/client-go/generated/containerized-data-importer/clientset/versioned/scheme"
	v1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/upload/v1beta1"
)

// UploadTokenRequestsGetter has a method to return a UploadTokenRequestInterface.
// A group's client should implement this interface.
type UploadTokenRequestsGetter interface {
	UploadTokenRequests(namespace string) UploadTokenRequestInterface
}

// UploadTokenRequestInterface has methods to work with UploadTokenRequest resources.
type UploadTokenRequestInterface interface {
	Create(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.CreateOptions) (*v1beta1.UploadTokenRequest, error)
	Update(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.UpdateOptions) (*v1beta1.UploadTokenRequest, error)
	UpdateStatus(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.UpdateOptions) (*v1beta1.UploadTokenRequest, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1beta1.UploadTokenRequest, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1beta1.UploadTokenRequestList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.UploadTokenRequest, err error)
	UploadTokenRequestExpansion
}

// uploadTokenRequests implements UploadTokenRequestInterface
type uploadTokenRequests struct {
	client rest.Interface
	ns     string
}

// newUploadTokenRequests returns a UploadTokenRequests
func newUploadTokenRequests(c *UploadV1beta1Client, namespace string) *uploadTokenRequests {
	return &uploadTokenRequests{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the uploadTokenRequest, and returns the corresponding uploadTokenRequest object, and an error if there is any.
func (c *uploadTokenRequests) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.UploadTokenRequest, err error) {
	result = &v1beta1.UploadTokenRequest{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of UploadTokenRequests that match those selectors.
func (c *uploadTokenRequests) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.UploadTokenRequestList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1beta1.UploadTokenRequestList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested uploadTokenRequests.
func (c *uploadTokenRequests) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a uploadTokenRequest and creates it.  Returns the server's representation of the uploadTokenRequest, and an error, if there is any.
func (c *uploadTokenRequests) Create(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.CreateOptions) (result *v1beta1.UploadTokenRequest, err error) {
	result = &v1beta1.UploadTokenRequest{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(uploadTokenRequest).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a uploadTokenRequest and updates it. Returns the server's representation of the uploadTokenRequest, and an error, if there is any.
func (c *uploadTokenRequests) Update(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.UpdateOptions) (result *v1beta1.UploadTokenRequest, err error) {
	result = &v1beta1.UploadTokenRequest{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		Name(uploadTokenRequest.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(uploadTokenRequest).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *uploadTokenRequests) UpdateStatus(ctx context.Context, uploadTokenRequest *v1beta1.UploadTokenRequest, opts v1.UpdateOptions) (result *v1beta1.UploadTokenRequest, err error) {
	result = &v1beta1.UploadTokenRequest{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		Name(uploadTokenRequest.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(uploadTokenRequest).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the uploadTokenRequest and deletes it. Returns an error if one occurs.
func (c *uploadTokenRequests) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *uploadTokenRequests) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched uploadTokenRequest.
func (c *uploadTokenRequests) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.UploadTokenRequest, err error) {
	result = &v1beta1.UploadTokenRequest{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("uploadtokenrequests").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
