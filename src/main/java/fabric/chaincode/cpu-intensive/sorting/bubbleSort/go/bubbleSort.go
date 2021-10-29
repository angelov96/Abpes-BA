package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type BubbleSortHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("bubbleSort-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started bubbleSort")

	var errStart = shim.Start(new(BubbleSortHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *BubbleSortHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *BubbleSortHandler) BubbleSort(stub shim.ChaincodeStubInterface, A []int, l int, r int, sig string) pb.Response {

	for i := r; i >= l+1; i-- {
		for j := l; j <= i-1; j++ {
			if A[j] > A[j+1] {
				exchange(A, j, j+1)
			}
		}
	}

	var errEvent = stub.SetEvent("sort/bubbleSort", []byte(sig))
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

func (self *BubbleSortHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
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

		return self.BubbleSort(stub, A, l, r, signature)
	}

	return shim.Error("Not yet implemented function called")
}
