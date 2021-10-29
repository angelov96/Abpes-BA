package main

import (
	"fmt"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type LoopHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("loop-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started loop")

	var errStart = shim.Start(new(LoopHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *LoopHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *LoopHandler) Loop(stub shim.ChaincodeStubInterface, start int, end int, sig string) pb.Response {

	for i := start; i < end; i++ {
		var _ = start + i
	}

	var errEvent = stub.SetEvent("loop", []byte(sig))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}

	return shim.Success(nil)
}

func (self *LoopHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "Loop" {

		if len(args) != 3 {
			return shim.Error("Call to Loop must have 3 parameters")
		}

		var start, errStart = strconv.Atoi(args[0])
		if errStart != nil {
			return shim.Error("Error while setting start")
		}
		var end, errEnd = strconv.Atoi(args[1])
		if errEnd != nil {
			return shim.Error("Error while setting end")
		}

		var signature = args[2]

		return self.Loop(stub, start, end, signature)
	}

	return shim.Error("Not yet implemented function called")

}
