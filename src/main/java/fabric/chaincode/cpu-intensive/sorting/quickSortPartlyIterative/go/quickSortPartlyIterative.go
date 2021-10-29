package main

import (
	"fmt"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type QuickSortPartlyIterativeHandler struct {
}

func main() {

	/*var file, errOpen = os.OpenFile("quickSortPartlyIterative-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started quickSortPartlyIterative")

	var errStart = shim.Start(new(QuickSortPartlyIterativeHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *QuickSortPartlyIterativeHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (self *QuickSortPartlyIterativeHandler) QuickSortPartlyIterative(A []int, l int, r int) {

	if l < r {
		var q = partition(A, l, r)
		if q == -1 {
			fmt.Printf("Error during chaincode: q == -1")
		}
		self.QuickSortPartlyIterative(A, l, q)
		self.QuickSortPartlyIterative(A, q+1, r)
	}

}

func partition(A []int, l int, r int) int {
	var x = A[((l + r) / 2)]
	var i = l - 1
	var j = r + 1

	for ok := true; ok; ok = true {
		for ok := true; ok; ok = A[j] > x {
			j--
		}

		for ok := true; ok; ok = A[i] < x {
			i++
		}

		if i < j {
			exchange(A, i, j)
		} else {
			return j
		}
	}
	return -1
}

func exchange(A []int, q int, i int) {
	var tmp = A[q]
	A[q] = A[i]
	A[i] = tmp
}

func (self *QuickSortPartlyIterativeHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
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

		self.QuickSortPartlyIterative(A, l, r)

		var errEvent = stub.SetEvent("sort/quickSortPartlyIterative", []byte(signature))
		if errEvent != nil {
			return shim.Error("Error setting event")
		}

		return shim.Success(nil)
	}

	return shim.Error("Not yet implemented function called")
}
