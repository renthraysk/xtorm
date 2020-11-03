package authentication

type Credentials interface {
	UserName() string
	Password() string
	Database() string
}

type Starter interface {
	Start(buf []byte, credentials Credentials) []byte
}

type StartContinuer interface {
	Starter
	Continue(buf []byte, credentials Credentials, authData []byte) []byte
}
