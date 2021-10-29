package main

import (
	"fmt"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type DoNothingHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("doNothing-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started doNothing")

	var errStart = shim.Start(new(DoNothingHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *DoNothingHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *DoNothingHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var _, args = stub.GetFunctionAndParameters()

	var errEvent = stub.SetEvent("doNothing", []byte(args[0]))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}
	return shim.Success(nil)
}
