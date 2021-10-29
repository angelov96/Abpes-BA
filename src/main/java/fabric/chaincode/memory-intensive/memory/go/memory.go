package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

const field string = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~"

type MemoryHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("memory-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started memory")

	var errStart = shim.Start(new(MemoryHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *MemoryHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func getChars(firstChar int, length int) string {

	var stateArr []byte
	for i := firstChar; i < length+firstChar; i++ {
		stateArr = append(stateArr, field[(firstChar+i)%len(field)])
	}
	return string(stateArr)
}

func (self *MemoryHandler) Memory(stub shim.ChaincodeStubInterface, lenOut int, lenIn int, firstChar int, length int, sig string) pb.Response {

	var arr = make([][]string, lenOut)
	for i := range arr {
		arr[i] = make([]string, lenIn)
	}

	for i := 0; i < lenOut; i++ {
		for j := 0; j < lenIn; j++ {
			arr[i][j] = getChars(firstChar+i+j, length+i+j)
		}
	}

	var errEvent = stub.SetEvent("memory", []byte(sig))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}

	return shim.Success(nil)
}

func (self *MemoryHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "Memory" {

		if len(args) != 5 {
			return shim.Error("Call to Memory must have 5 parameters")
		}

		var lenOut, errOut = strconv.Atoi(args[0])
		if errOut != nil {
			return shim.Error("Error while setting lenOut")
		}
		var lenIn, errIn = strconv.Atoi(args[1])
		if errIn != nil {
			return shim.Error("Error while setting lenIn")
		}
		var firstChar, errFirst = strconv.Atoi(args[2])
		if errFirst != nil {
			return shim.Error("Error while setting firstChar")
		}
		var length, errLength = strconv.Atoi(args[3])
		if errLength != nil {
			return shim.Error("Error while setting length")
		}

		var signature = args[4]

		return self.Memory(stub, lenOut, lenIn, firstChar, length, signature)
	}

	return shim.Error("Not yet implemented function called")

}
