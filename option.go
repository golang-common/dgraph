package dgraph

type Option func(client *Client)

func WithTls(certFile, servname string) Option {
	return func(client *Client) {
		client.certFile = certFile
		client.servname = servname
	}
}

func WithAuth(username, password string, namespace uint64) Option {
	return func(client *Client) {
		client.username = username
		client.password = password
		client.namespace = namespace
	}
}