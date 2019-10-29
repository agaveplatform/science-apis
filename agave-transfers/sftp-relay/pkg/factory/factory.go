package factory

//import (
//	//relay "github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/pkg/sftprelay"
//)
//
//type FileTransfer struct {
//	params relay.ConnParams
//}
//
//type TransferFactory interface {
//	ReadFrom() FileTransfer
//}
//
//type SFTP_IN_Factory struct{}
//type SFTP_OUT_Factory struct{}
//type SFTP_SFTP_Factory struct{}
//
//// main factory method
//func GetSourceType(typeTxfr string) FileTransfer {
//
//	var txfrFact TransferFactory
//
//	switch typeTxfr {
//	case "SFTP":
//		txfrFact = getTxfrFact()
//		return txfrFact.ReadFrom()
//		//case "LOCAL-SFTP":
//		//	txfrFact = LOCAL_Factory{}
//		//	return txfrFact.CreateFileTxfr()
//		//case "SFTP-SFTP":
//		//	txfrFact = SFTP_SFTP_Factory{}
//		//	return txfrFact.CreateFileTxfr()
//	}
//	return FileTransfer{}
//}

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

//func main() {
//
//	//var typeGf string
//	//typeGf = "Indian"
//	//a := getTxfrType(typeGf)
//
//	//fmt.Println(a)
//}
//func getTxfrFact() (params relay.ConnParams) {
//	params = relay.ConnParams{}
//
//	return params
//}
