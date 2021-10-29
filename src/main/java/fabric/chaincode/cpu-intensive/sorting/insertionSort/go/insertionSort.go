package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type InsertionSortHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("insertionSort-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started insertionSort")

	var errStart = shim.Start(new(InsertionSortHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *InsertionSortHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *InsertionSortHandler) InsertSort(stub shim.ChaincodeStubInterface, A []int, l int, r int, sig string) pb.Response {

	for i := l; i <= r-1; i++ {
		for j := i; j >= l; j-- {
			if A[j] > A[j+1] {
				exchange(A, j, j+1)
			}
		}
	}

	var errEvent = stub.SetEvent("sort/insertionSort", []byte(sig))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}

	return shim.Success(nil)
}

func exchange(A []int, q int, i int) {
	var tmp = A[q]
	A[q] = A[i]
	A[i] = tmp
}

func (self *InsertionSortHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "Sort" {

		if len(args) < 4 {
			return shim.Error("Call to Sort must have at least 4 parameters")
		}

		var A = make([]int, len(args)-3)
		for i := range A {
			A[i], _ = strconv.Atoi(args[i])
		}

		var l, errL = strconv.Atoi(args[len(A)])
		if errL != nil {
			return shim.Error("Error while setting L in Sort")
		}
		var r, errR = strconv.Atoi(args[len(A)+1])
		if errR != nil {
			return shim.Error("Error while setting R in Sort")
		}

		var signature = args[len(A)+2]

		return self.InsertSort(stub, A, l, r, signature)
	}

	return shim.Error("Not yet implemented function called")
}
