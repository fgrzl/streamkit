package api

type BidiStream interface {
	Encode(m any) error
	Decode(m any) (err error)
	CloseSend(error) error
	Close(error)
	EndOfStreamError() error
}
