package main

import (
	"fmt"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"

	"strconv"
)

//"github.com/hyperledger/fabric/core/chaincode/shim"
//pb "github.com/hyperledger/fabric/protos/peer"

type SmallBankHandler struct {
}

const checkingSuffix string = "_checking"

const savingsSuffix string = "_savings"

func main() {

	/*var file, errOpen = os.OpenFile("smallBank-fabric.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errOpen != nil {
		log.Println(errOpen)
	}
	defer file.Close()

	log.SetOutput(file)*/

	fmt.Println("Started smallbank")

	var errStart = shim.Start(new(SmallBankHandler))
	if errStart != nil {
		fmt.Printf("Error starting chaincode: %v \n", errStart)
	}

}

func (self *SmallBankHandler) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func getSavings(a string) string {
	return a + savingsSuffix
}

func getChecking(a string) string {
	return a + checkingSuffix
}

func (self *SmallBankHandler) WriteCheck(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var acctId = args[0]
	var stateSavings, errGetSavings = stub.GetState(getSavings(acctId))
	if errGetSavings != nil {
		return shim.Error("Error getting the state")
	}

	var stateChecking, errGetChecking = stub.GetState(getChecking(acctId))
	if errGetChecking != nil {
		return shim.Error("Error getting the state")
	}

	var amount, errAmountConvert = strconv.Atoi(args[1])
	if errAmountConvert != nil {
		return shim.Error("Failed convert amount")
	}
	if stateSavings == nil || stateChecking == nil || amount < 0 {
		var signature = args[2]
		var errEvent = stub.SetEvent("writeCheck/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var savings, errSavingConvert = strconv.Atoi(string(stateSavings))
	if errSavingConvert != nil {
		return shim.Error("Failed convert saving")
	}
	var checks, errCheckConvert = strconv.Atoi(string(stateChecking))
	if errCheckConvert != nil {
		return shim.Error("Failed convert check")
	}
	var total = savings + checks

	if total < amount {
		var check = strconv.Itoa(checks - amount - 1)
		var errSet = stub.PutState(getChecking(acctId), []byte(check))
		if errSet != nil {
			return shim.Error("Cannot put state")
		}
	} else {
		var check = strconv.Itoa(checks - amount)
		var errSet = stub.PutState(getChecking(acctId), []byte(check))
		if errSet != nil {
			return shim.Error("Cannot put state")
		}
	}
	var signature = args[2]
	var errEvent = stub.SetEvent("writeCheck", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	return shim.Success(nil)
}

func (self *SmallBankHandler) DepositChecking(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var acctId = args[0]

	var stateChecking, errGetChecking = stub.GetState(getChecking(acctId))
	if errGetChecking != nil {
		return shim.Error("Error getting the state")
	}

	if stateChecking == nil {
		var signature = args[2]
		var errEvent = stub.SetEvent("depositChecking/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var currentAmount, errCurrentAmountConvert = strconv.Atoi(string(stateChecking))
	if errCurrentAmountConvert != nil {
		return shim.Error("Failed convert current amount")
	}

	var amount, errAmountConvert = strconv.Atoi(args[1])
	if errAmountConvert != nil {
		return shim.Error("Failed convert amount")
	}
	currentAmount += amount

	var newAmount = strconv.Itoa(currentAmount)
	var errSet = stub.PutState(getChecking(acctId), []byte(newAmount))
	if errSet != nil {
		return shim.Error("Cannot put state")
	}

	var signature = args[2]
	var errEvent = stub.SetEvent("depositChecking", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	return shim.Success(nil)
}

func (self *SmallBankHandler) TransactSavings(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var acctId = args[0]
	var stateSavings, errGetSavings = stub.GetState(getSavings(acctId))
	if errGetSavings != nil {
		return shim.Error("Error getting the state")
	}

	if stateSavings == nil {
		var signature = args[2]
		var errEvent = stub.SetEvent("transactSavings/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var savings, errSavingConvert = strconv.Atoi(string(stateSavings))
	if errSavingConvert != nil {
		return shim.Error("Failed convert saving")
	}

	var amount, errAmountConvert = strconv.Atoi(args[1])
	if errAmountConvert != nil {
		return shim.Error("Failed convert amount")
	}
	var balance = savings - amount
	if balance < 0 {
		var signature = args[2]
		var errEvent = stub.SetEvent("transactSavings/error/balance", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	savings -= amount
	var newAmount = strconv.Itoa(savings)

	var errSet = stub.PutState(getSavings(acctId), []byte(newAmount))
	if errSet != nil {
		return shim.Error("Cannot put state")
	}

	var signature = args[2]
	var errEvent = stub.SetEvent("transactSavings", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	return shim.Success(nil)
}

func (self *SmallBankHandler) SendPayment(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var sender = args[0]
	var destination = args[1]
	var amount, errAmountConvert = strconv.Atoi(args[2])
	if errAmountConvert != nil {
		return shim.Error("Failed convert amount")
	}
	var stateSavingsSender, errGetSavingsSender = stub.GetState(getSavings(sender))
	if errGetSavingsSender != nil {
		return shim.Error("Error getting the state")
	}
	var stateCheckingSender, errGetCheckingSender = stub.GetState(getChecking(sender))
	if errGetCheckingSender != nil {
		return shim.Error("Error getting the state")
	}

	var stateSavingsDestination, errGetSavingsDestination = stub.GetState(getSavings(destination))
	if errGetSavingsDestination != nil {
		return shim.Error("Error getting the state")
	}
	var stateCheckingDestination, errGetCheckingDestination = stub.GetState(getChecking(destination))
	if errGetCheckingDestination != nil {
		return shim.Error("Error getting the state")
	}

	if stateCheckingSender == nil || stateCheckingDestination == nil || stateSavingsSender == nil || stateSavingsDestination == nil {
		var signature = args[3]
		var errEvent = stub.SetEvent("sendPayment/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var checkingBalanceSender, errCheckingBalanceSender = strconv.Atoi(string(stateCheckingSender))
	if errCheckingBalanceSender != nil {
		return shim.Error("Failed convert amount")
	}
	if checkingBalanceSender < amount {
		var signature = args[3]
		var errEvent = stub.SetEvent("sendPayment/error/checkingBalanceSender", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var newAmountSender, errNewAmountSender = strconv.Atoi(string(stateCheckingSender))
	if errNewAmountSender != nil {
		return shim.Error("Failed convert amount")
	}
	var newAmountDestination, errNewAmountDestination = strconv.Atoi(string(stateCheckingDestination))
	if errNewAmountDestination != nil {
		return shim.Error("Failed convert amount")
	}

	newAmountSender -= amount
	newAmountDestination += amount

	var newAmountSenderPut = strconv.Itoa(newAmountSender)
	var newAmountDestinationPut = strconv.Itoa(newAmountDestination)

	var errSetSender = stub.PutState(getChecking(sender), []byte(newAmountSenderPut))
	if errSetSender != nil {
		return shim.Error("Cannot put state")
	}

	var errSetDestination = stub.PutState(getChecking(destination), []byte(newAmountDestinationPut))
	if errSetDestination != nil {
		return shim.Error("Cannot put state")
	}

	var signature = args[3]
	var errEvent = stub.SetEvent("sendPayment", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	return shim.Success(nil)
}

func (self *SmallBankHandler) Balance(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var acctId = args[0]

	var stateSavings, errGetSavings = stub.GetState(getSavings(acctId))
	if errGetSavings != nil {
		return shim.Error("Error getting the state")
	}

	var stateChecking, errGetChecking = stub.GetState(getChecking(acctId))
	if errGetChecking != nil {
		return shim.Error("Error getting the state")
	}

	if stateSavings == nil || stateChecking == nil {
		var signature = args[1]
		var errEvent = stub.SetEvent("balance/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		//var zero = strconv.Itoa(0)
		//return shim.Success([]byte(zero))
		return shim.Success(nil)
	}

	var savings, errSavingsConvert = strconv.Atoi(string(stateSavings))
	if errSavingsConvert != nil {
		return shim.Error("Failed convert savings")
	}
	var checkings, errCheckingsConvert = strconv.Atoi(string(stateChecking))
	if errCheckingsConvert != nil {
		return shim.Error("Failed convert checkings")
	}

	//var total = savings + checkings
	var _ = savings + checkings

	var signature = args[1]
	var errEvent = stub.SetEvent("balance", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	//return shim.Success([]byte(strconv.Itoa(total)))
	return shim.Success(nil)
}

func (self *SmallBankHandler) Amalgamate(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var acctId0 = args[0]
	var acctId1 = args[1]

	var stateSavings0, errGetSavings0 = stub.GetState(getSavings(acctId0))
	if errGetSavings0 != nil {
		return shim.Error("Error getting the state")
	}
	var stateChecking0, errGetChecking0 = stub.GetState(getChecking(acctId0))
	if errGetChecking0 != nil {
		return shim.Error("Error getting the state")
	}

	var stateSavings1, errGetSavings1 = stub.GetState(getSavings(acctId1))
	if errGetSavings1 != nil {
		return shim.Error("Error getting the state")
	}
	var stateChecking1, errGetChecking1 = stub.GetState(getChecking(acctId1))
	if errGetChecking1 != nil {
		return shim.Error("Error getting the state")
	}

	if stateChecking0 == nil || stateChecking1 == nil || stateSavings0 == nil || stateSavings1 == nil {
		var signature = args[2]
		var errEvent = stub.SetEvent("amalgamate/error", []byte(signature))
		if errEvent != nil {
			return shim.Error("Cannot set event")
		}
		return shim.Success(nil)
	}

	var savings0, errSavingsConvert0 = strconv.Atoi(string(stateSavings0))
	if errSavingsConvert0 != nil {
		return shim.Error("Failed convert savings")
	}
	var checkings0, errCheckingsConvert0 = strconv.Atoi(string(stateChecking0))
	if errCheckingsConvert0 != nil {
		return shim.Error("Failed convert savings")
	}

	var total = savings0 + checkings0
	//assert(total >= 0)

	var zero = "0"
	var errSetAcctChecking0 = stub.PutState(getChecking(acctId0), []byte(zero))
	if errSetAcctChecking0 != nil {
		return shim.Error("Cannot put state")
	}
	var errSetAcctSavings0 = stub.PutState(getSavings(acctId0), []byte(zero))
	if errSetAcctSavings0 != nil {
		return shim.Error("Cannot put state")
	}

	var checkings1, errCheckingsConvert1 = strconv.Atoi(string(stateChecking1))
	if errCheckingsConvert1 != nil {
		return shim.Error("Failed convert checkings")
	}

	checkings1 += total
	var checkingsNew = strconv.Itoa(checkings1)
	var errSetAcct1 = stub.PutState(getChecking(acctId1), []byte(checkingsNew))
	if errSetAcct1 != nil {
		return shim.Error("Cannot put state")
	}

	var signature = args[2]
	var errEvent = stub.SetEvent("amalgamate", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	//return shim.Success([]byte(strconv.Itoa(total)))
	return shim.Success(nil)
}

func (self *SmallBankHandler) CreateAccount(stub shim.ChaincodeStubInterface, args []string) pb.Response {

	var acctIdChecking = args[0]
	var acctIdSavings = args[1]
	var checking = args[2]
	var savings = args[3]
	var errSetChecking = stub.PutState(getChecking(acctIdChecking), []byte(checking))
	if errSetChecking != nil {
		return shim.Error("Cannot put state")
	}
	var errSetSavings = stub.PutState(getSavings(acctIdSavings), []byte(savings))
	if errSetSavings != nil {
		return shim.Error("Cannot put state")
	}

	var signature = args[4]
	var errEvent = stub.SetEvent("createAccount", []byte(signature))
	if errEvent != nil {
		return shim.Error("Cannot set event")
	}
	return shim.Success(nil)
}

func (self *SmallBankHandler) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	var function, args = stub.GetFunctionAndParameters()

	if function == "WriteCheck" {
		return self.WriteCheck(stub, args)
	}
	if function == "DepositChecking" {
		return self.DepositChecking(stub, args)
	}
	if function == "TransactSavings" {
		return self.TransactSavings(stub, args)
	}
	if function == "SendPayment" {
		return self.SendPayment(stub, args)
	}
	if function == "Balance" {
		return self.Balance(stub, args)
	}
	if function == "Amalgamate" {
		return self.Amalgamate(stub, args)
	}
	if function == "CreateAccount" {
		return self.CreateAccount(stub, args)
	}

	return shim.Error("Not yet implemented function called")

}
