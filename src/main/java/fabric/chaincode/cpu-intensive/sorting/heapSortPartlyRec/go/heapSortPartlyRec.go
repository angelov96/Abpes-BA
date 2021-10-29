package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type HeapSortPartlyRecHandler struct {
}

const k int = 2 // 2, 3

func main() {

	/*var file, errOpen = os.OpenFile("heapSortPartlyRec-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started heapSortPartlyRec")

	var errStart = shim.Start(new(HeapSortPartlyRecHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *HeapSortPartlyRecHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *HeapSortPartlyRecHandler) HeapSortPartlyRec(stub shim.ChaincodeStubInterface, A []int, l int, r int, sig string) pb.Response {

	buildheap(A, l, r)
	heapsort2(A, l, r)

	var errEvent = stub.SetEvent("sort/heapSortPartlyRec", []byte(sig))
	if errEvent != nil {
		return shim.Error("Error setting event")
	}

	return shim.Success(nil)
}

func heapsort2(A []int, l int, r int) {
	if r > l {
		exchange(A, l, r)
		heapify(A, l, l, r-1)
		heapsort2(A, l, r-1)
	}
}

func buildheap(A []int, l int, r int) {
	buildheap_helper(A, l, r, (r-l-1)/k)
}

func buildheap_helper(A []int, l int, r int, i int) {
	if i >= 0 {
		heapify(A, l, l+i, r)
		buildheap_helper(A, l, r, i-1)
	}
}

func heapify(A []int, l int, q int, r int) {
	var largest = l + k*(q-l) + 1
	if largest <= r {

		for i := largest + 1; i <= largest+k-1; i++ {
			if i <= r && A[i] > A[largest] {
				largest = i
			}

			if A[largest] > A[q] {
				exchange(A, largest, q)
				heapify(A, l, largest, r)
			}
		}
	}
}

func exchange(A []int, q int, i int) {
	var tmp = A[q]
	A[q] = A[i]
	A[i] = tmp
}

func (self *HeapSortPartlyRecHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
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

		return self.HeapSortPartlyRec(stub, A, l, r, signature)
	}

	return shim.Error("Not yet implemented function called")
}
