package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"

	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
)

func main() {
	proposal := &kronospb.OracleProposal{}
	// read line by line
	sc := bufio.NewScanner(os.Stdin)

	for sc.Scan() {
		str := sc.Text()
		foo, err := hex.DecodeString(str)
		if err != nil {
			panic(err)
		}
		err = protoutil.Unmarshal(foo, proposal)
		if err != nil {
			panic(err)
		}
		fmt.Println("OK|" + proposal.String())
	}

}
