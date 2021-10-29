package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type SelectionSortHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("selectionSortRec-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started selectionSortRec")

	var errStart = shim.Start(new(SelectionSortHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *SelectionSortHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *SelectionSortHandler) SelectionSortRec(A []int, l int, r int) {

	if l < r {
		var q = max(A, l, l+1, r)
		exchange(A, q, r)
		self.SelectionSortRec(A, l, r-1)
	}

}

func max(A []int, q int, l int, r int) int {
	if l <= r {
		if A[l] > A[q] {
			return max(A, l, l+1, r)
		} else {
			return max(A, q, l+1, r)
		}
	} else {
		return q
	}
}

func exchange(A []int, q int, i int) {
	var tmp = A[q]
	A[q] = A[i]
	A[i] = tmp
}

func (self *SelectionSortHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
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

		self.SelectionSortRec(A, l, r)

		var errEvent = stub.SetEvent("sort/selectionSortRec", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error setting event")
		}

		return shim.Success(nil)
	}

	return shim.Error("Not yet implemented function called")
}
