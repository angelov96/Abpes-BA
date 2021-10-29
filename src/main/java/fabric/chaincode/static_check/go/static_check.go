package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type StaticCheckHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("static-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started static_check")

	var errStart = shim.Start(new(StaticCheckHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

var checkVar = true

func (self *StaticCheckHandler) CheckVarSet(val string) pb.Response {
	var checkExistingVar, errParse = strconv.ParseBool(val)
	checkVar = checkExistingVar
	if errParse != nil {
		return shim.Error("Could not parse boolean")
	}
	log.Println("New checkVar: ")
	log.Println(checkVar)
	return shim.Success(nil)
}

func (self *StaticCheckHandler) CheckVarGet() pb.Response {
	log.Println("checkVar: ")
	log.Println(checkVar)
	if checkVar {
		//return shim.Success([]byte("true"))
		return shim.Success(nil)
	} else {
		//return shim.Success([]byte("false"))
		return shim.Success(nil)
	}
}

func (self *StaticCheckHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *StaticCheckHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "CheckVarSet" {
		return self.CheckVarSet(args[0])
	}
	if function == "CheckVarGet" {
		return self.CheckVarGet()
	}

	return shim.Error("Not yet implemented function called")
}
