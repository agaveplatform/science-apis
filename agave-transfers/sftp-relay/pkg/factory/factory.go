package factory

import (
	relay "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
	"golang.org/x/crypto/ssh"
)

type FileTransfer struct {
	params relay.ConnParams
	conn   *ssh.Client
}

type TransferFactory interface {
	ReadFrom() FileTransfer
}

type SFTP_IN_Factory struct{}
type SFTP_OUT_Factory struct{}
type SFTP_SFTP_Factory struct{}

// main factory method
func GetSourceType(typeTxfr string) FileTransfer {

	var txfrFact TransferFactory

	switch typeTxfr {
	case "SFTP":
		txfrFact, _ = getTxfrFact()
		return txfrFact.ReadFrom()
		//case "LOCAL-SFTP":
		//	txfrFact = LOCAL_Factory{}
		//	return txfrFact.CreateFileTxfr()
		//case "SFTP-SFTP":
		//	txfrFact = SFTP_SFTP_Factory{}
		//	return txfrFact.CreateFileTxfr()
	}
	return FileTransfer{}
}

//func GetDestType(typeTxfr string) FileTransfer {
//
//	var txfrFact TransferFactory
//
//	switch typeTxfr {
//	case "SFTP-Local":
//		txfrFact =  SFTP_IN_Factory{}
//		return txfrFact.CreateFileTxfr()
//	case "LOCAL-SFTP":
//		txfrFact = LOCAL_Factory{}
//		return txfrFact.CreateFileTxfr()
//	case "SFTP-SFTP":
//		txfrFact = SFTP_SFTP_Factory{}
//		return txfrFact.CreateFileTxfr()
//	}
//	return FileTransfer{}
//}

func main() {

	//var typeGf string
	//typeGf = "Indian"
	//a := getTxfrType(typeGf)

	//fmt.Println(a)
}
func getTxfrFact() (params relay.ConnParams, conn *ssh.Client) {
	params = relay.ConnParams{}
	conn = relay.Conn

	return params, conn
}
