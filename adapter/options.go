package adapter

type Options struct {
	ConnectorType      ConnectorType
	Addresses          []string
	Username, Password string
}

type Option func(*Options)

func WithConnectorType(t ConnectorType) Option {
	return func(opt *Options) {
		opt.ConnectorType = t
	}
}

func WithAddresses(addresses []string) Option {
	return func(opt *Options) {
		opt.Addresses = addresses
	}
}

func WithAuth(username, password string) Option {
	return func(opt *Options) {
		opt.Username = username
		opt.Password = password
	}
}
