package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type RecursionHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("recursion-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started recursion")

	var errStart = shim.Start(new(RecursionHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *RecursionHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *RecursionHandler) Recursion(start int, end int) {

	if start != end {
		self.Recursion(start, end-1)
	}

}

func (self *RecursionHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "Recursion" {

		if len(args) != 3 {
			return shim.Error("Call to Recursion must have 3 parameters")
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

		self.Recursion(start, end)

		var errEvent = stub.SetEvent("recursion", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error setting event")
		}

		return shim.Success(nil)
	}

	return shim.Error("Not yet implemented function called")

}
