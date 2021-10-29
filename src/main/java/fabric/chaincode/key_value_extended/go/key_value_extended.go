package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type KeyValueExtendedHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("kv-extended-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started key_value_extended")

	var errStart = shim.Start(new(KeyValueExtendedHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *KeyValueExtendedHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *KeyValueExtendedHandler) Get(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return shim.Error("Call to Get must have 2 parameters")
	}

	var value, errGet = stub.GetState(args[0])
	if errGet != nil {
		return shim.Error("Error while GetState call")
	}

	if value == nil {
		var signature = args[1]
		var errEvent = stub.SetEvent("keyValueEx/get/doesntexist", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error while setting event in GetState call")
		}
	} else {
		var signature = args[1]
		var errSet = stub.SetEvent("keyValueEx/get", []byte(signature))
		if errSet != nil {
			return shim.Error("Error while setting event in GetState call")
		}
	}
	return shim.Success(nil)
}

func (self *KeyValueExtendedHandler) Set(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 3 {
		return shim.Error("Call to Set must have 3 parameters")
	}

	var key = args[0]
	var value = []byte(args[1])

	var checkKey, errGet = stub.GetState(key)
	if errGet != nil {
		return shim.Error("Error while getting state in Set")
	}
	if checkKey != nil {
		var signature = args[2]
		var errEvent = stub.SetEvent("keyValueEx/set/exist", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error while setting event in SetState call")
		}
	} else {
		var errSet = stub.PutState(key, value)
		if errSet != nil {
			return shim.Error("Error while setting state")
		}

		var signature = args[2]
		var errEvent = stub.SetEvent("keyValueEx/set", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error while setting event in SetState call")
		}

	}
	return shim.Success(nil)
}

func (self *KeyValueExtendedHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "Get" {
		return self.Get(stub, args)
	}
	if function == "Set" {
		return self.Set(stub, args)
	}

	return shim.Error("Not yet implemented function called")
}
