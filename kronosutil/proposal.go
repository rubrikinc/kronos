package kronosutil

import (
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
)

func IsOracleProposal(prop []byte) bool {
	_, err := OracleProposalFromBytes(prop)
	return err == nil
}

func OracleProposalFromBytes(prop []byte) (*kronospb.OracleProposal, error) {
	proposal := &kronospb.OracleProposal{}
	if err := protoutil.Unmarshal(prop, proposal); err != nil {
		return nil, err
	}
	return proposal, nil
}
