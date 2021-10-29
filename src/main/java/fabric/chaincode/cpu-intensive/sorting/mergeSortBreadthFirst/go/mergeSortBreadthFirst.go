package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type MergeSortHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("mergeSortBreadthFirst-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started mergeSortBreadthFirst")

	var errStart = shim.Start(new(MergeSortHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *MergeSortHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

// even only **2
func (self *MergeSortHandler) MergeSort(stub shim.ChaincodeStubInterface, A []int, l int, r int, sig string) pb.Response {

	for m := 1; m <= r-l; m = m + m {
		for i := l; i <= r; i = i + m*2 {
			merge(A, i, i+m-1, i+2*m-1)
		}
	}

	var errEvent = stub.SetEvent("sort/mergeSortBreadthFirst", []byte(sig))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}

	return shim.Success(nil)
}

func merge(A []int, l int, q int, r int) {
	var B = make([]int, len(A))
	for i := l; i <= q; i++ {
		B[i] = A[i]
	}
	for j := q + 1; j <= r; j++ {
		B[(r + q + 1 - j)] = A[j]
	}
	var s = l
	var t = r
	for k := l; k <= r; k++ {
		if B[s] <= B[t] {
			A[k] = B[s]
			s++
		} else {
			A[k] = B[t]
			t--
		}
	}
}

func (self *MergeSortHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
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

		return self.MergeSort(stub, A, l, r, signature)
	}

	return shim.Error("Not yet implemented function called")
}
