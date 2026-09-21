package core

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
)

func TestValidateAddresses(t *testing.T) {
	for _, v := range []string{"", ":7410", "10.1.2.74:7410", "[::1]:7410", "node-3.shale.svc:7410", "davy:1"} {
		require.NoError(t, validateAddr("data_address", v), v)
	}
	for _, v := range []string{"7410", "10.1.2.74", "host:0", "host:70000", "a b:1", "bad_host:1", "-x.y:1", "http://host:1", "host:1/path"} {
		require.Error(t, validateAddr("data_address", v), v)
	}
	require.NoError(t, validateInterfaces([]*api.HostInterface{
		api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.74/24", "fe80::1/64", "192.168.0.9"}}.Build(),
	}))
	require.Error(t, validateInterfaces([]*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.74/24", "evil.example.com"}}.Build()}))
	require.Error(t, validateInterfaces([]*api.HostInterface{api.HostInterface_builder{Name: "eth\x00", Addresses: nil}.Build()}))
	require.NoError(t, validateNodeAddresses(nil, ":7411", "10.1.2.74:7410"))
	require.Error(t, validateNodeAddresses(nil, ":7411", "10.1.2.74:7410?x=1"))
	require.Error(t, validateRelayAddresses(nil, "relay:7430", "relay:99999"))
}

// A URL is put together by the URL package: the key's segments stay,
// the token is encoded, and an IPv6 host is bracketed.
func TestEndpointURL(t *testing.T) {
	ep := api.Endpoint_builder{Scheme: "https", Host: "10.1.2.74", Port: 7410}.Build()
	require.Equal(t, "https://10.1.2.74:7410/laminae/2026/09/21/05/a.b?token=Shale.x-y_z", endpointURL(ep, "laminae/2026/09/21/05/a.b", url.Values{"token": {"Shale.x-y_z"}}))
	require.Equal(t, "https://10.1.2.74:7410/laminae/a?token=a%26b%3Fc", endpointURL(ep, "/laminae/a", url.Values{"token": {"a&b?c"}}))
	six := api.Endpoint_builder{Scheme: "http", Host: "fe80::1", Port: 7440}.Build()
	require.Equal(t, "http://[fe80::1]:7440/whep/01a0", endpointURL(six, "whep/01a0", nil))
}
