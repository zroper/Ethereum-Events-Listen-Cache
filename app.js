/**
 * @Author Zachary Roper, zachroper@gmail.com
 *
 * Synchronizes data for MongoDB with ERC-1155 contract events
 * by using web3 and Infura. to look at past completed block events
*/

const Web3 = require('web3');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const Db = require('mongodb').Db;
const uri = "mongodb+srv://mzkzUser:zW0Jx7me5JkWtEaJ@mzkz-dt9ay.mongodb.net/test?retryWrites=true";
const options = {
    keepAlive: 300000, 
	connectTimeoutMS: 30000,
	useNewUrlParser: true
};
const client = new MongoClient(uri, options);
const db = new Db('mzkz', client);

//import bodyParser from 'body-parser';

// Set up Web3 and contract connection
//const web3 = new Web3(new Web3.providers.HttpProvider('http://localhost:7545/'));
const web3 = new Web3(new Web3.providers.HttpProvider('https://mainnet.infura.io/v3/fc97d03ee3b4448fbaf37ce7a19ec564'));
const abi = [{"payable":false,"stateMutability":"nonpayable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_spender","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_block","type":"uint256"},{"indexed":false,"name":"_storage","type":"address"},{"indexed":false,"name":"_oldContract","type":"address"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"_block","type":"uint256"},{"indexed":false,"name":"_nextContract","type":"address"}],"name":"Retire","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_from","type":"address"},{"indexed":false,"name":"_data","type":"string"}],"name":"Log","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateDecimals","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateName","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateSymbol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":false,"name":"_uri","type":"string"}],"name":"SetURI","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"}],"name":"Assign","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_creator","type":"address"}],"name":"AcceptAssignment","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_creator","type":"address"},{"indexed":false,"name":"_isNonFungible","type":"bool"}],"name":"Create","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateMaxMeltFee","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateMeltFee","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_operator","type":"address"},{"indexed":true,"name":"_id","type":"uint256"},{"indexed":false,"name":"_approved","type":"bool"}],"name":"OperatorApproval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_sender","type":"address"},{"indexed":true,"name":"_feeId","type":"uint256"},{"indexed":false,"name":"_feeValue","type":"uint256"}],"name":"TransferFee","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateMaxTransferFee","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateTransferable","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"}],"name":"UpdateTransferFee","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_account","type":"address"},{"indexed":false,"name":"_whitelisted","type":"address"},{"indexed":false,"name":"_on","type":"bool"}],"name":"Whitelist","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_owner","type":"address"},{"indexed":false,"name":"_value","type":"uint256"}],"name":"Melt","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_id","type":"uint256"},{"indexed":true,"name":"_sender","type":"address"}],"name":"DeployERCAdapter","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_tradeId","type":"uint256"},{"indexed":true,"name":"_firstParty","type":"address"},{"indexed":true,"name":"_secondParty","type":"address"},{"indexed":false,"name":"_escrowedEnjFirstParty","type":"uint256"}],"name":"CreateTrade","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_tradeId","type":"uint256"},{"indexed":true,"name":"_firstParty","type":"address"},{"indexed":true,"name":"_secondParty","type":"address"},{"indexed":false,"name":"_receivedEnjFirstParty","type":"uint256"},{"indexed":false,"name":"_changeEnjFirstParty","type":"uint256"},{"indexed":false,"name":"_receivedEnjSecondParty","type":"uint256"}],"name":"CompleteTrade","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_tradeId","type":"uint256"},{"indexed":true,"name":"_firstParty","type":"address"},{"indexed":false,"name":"_receivedEnjFirstParty","type":"uint256"}],"name":"CancelTrade","type":"event"},{"constant":true,"inputs":[{"name":"_interfaceID","type":"bytes4"}],"name":"supportsInterface","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"name":"_name","type":"string"},{"name":"_totalSupply","type":"uint256"},{"name":"_initialReserve","type":"uint256"},{"name":"_supplyModel","type":"address"},{"name":"_meltValue","type":"uint256"},{"name":"_meltFeeRatio","type":"uint16"},{"name":"_transferable","type":"uint8"},{"name":"_transferFeeSettings","type":"uint256[3]"},{"name":"_nonFungible","type":"bool"}],"name":"create","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_initialReserve","type":"uint256"}],"name":"minMeltValue","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_to","type":"address[]"},{"name":"_values","type":"uint256[]"}],"name":"mintFungibles","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_to","type":"address[]"}],"name":"mintNonFungibles","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_to","type":"address[]"},{"name":"_data","type":"uint128[]"}],"name":"mintNonFungiblesWithData","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"reserve","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_value","type":"uint128"}],"name":"releaseReserve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_name","type":"string"}],"name":"updateName","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_creator","type":"address"}],"name":"assign","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"}],"name":"acceptAssignment","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_account","type":"address"},{"name":"_whitelisted","type":"address"},{"name":"_on","type":"bool"}],"name":"setWhitelisted","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_transferable","type":"uint8"}],"name":"setTransferable","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_fee","type":"uint16"}],"name":"setMeltFee","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_fee","type":"uint16"}],"name":"decreaseMaxMeltFee","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_fee","type":"uint256"}],"name":"setTransferFee","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_fee","type":"uint256"}],"name":"decreaseMaxTransferFee","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_decimals","type":"uint8"},{"name":"_symbol","type":"string"}],"name":"deployERC20Adapter","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_symbol","type":"string"}],"name":"deployERC721Adapter","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_data","type":"string"}],"name":"addLog","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"typeCount","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_index","type":"uint256"}],"name":"typeByIndex","outputs":[{"name":"_id","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"nonFungibleTypeCount","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_index","type":"uint256"}],"name":"nonFungibleTypeByIndex","outputs":[{"name":"_id","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"fungibleTypeCount","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_index","type":"uint256"}],"name":"fungibleTypeByIndex","outputs":[{"name":"_id","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"typeData","outputs":[{"name":"_name","type":"string"},{"name":"_creator","type":"address"},{"name":"_meltValue","type":"uint256"},{"name":"_meltFeeRatio","type":"uint16"},{"name":"_meltFeeMaxRatio","type":"uint16"},{"name":"_supplyModel","type":"address"},{"name":"_totalSupply","type":"uint256"},{"name":"_circulatingSupply","type":"uint256"},{"name":"_reserve","type":"uint256"},{"name":"_transferable","type":"uint8"},{"name":"_transferFeeData","type":"uint256[4]"},{"name":"_nonFungible","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"transferSettings","outputs":[{"name":"_transferable","type":"uint8"},{"name":"_transferFeeType","type":"uint8"},{"name":"_transferFeeCurrency","type":"uint256"},{"name":"_transferFeeValue","type":"uint256"},{"name":"_transferFeeMaxValue","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_creator","type":"address"}],"name":"isCreatorOf","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_account","type":"address"},{"name":"_whitelisted","type":"address"}],"name":"whitelisted","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"mintableSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"circulatingSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"getERC20Adapter","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"getERC721Adapter","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_data","type":"bytes"}],"name":"safeTransfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_msgSender","type":"address"}],"name":"transferAdapter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_msgSender","type":"address"}],"name":"transferFromAdapter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"batchTransfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"},{"name":"_data","type":"bytes"}],"name":"safeBatchTransfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"batchTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"},{"name":"_data","type":"bytes"}],"name":"safeBatchTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address[]"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"multicastTransfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address[]"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"},{"name":"_data","type":"bytes"}],"name":"safeMulticastTransfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_from","type":"address[]"},{"name":"_to","type":"address[]"},{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"multicastTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_id","type":"uint256"},{"name":"_currentValue","type":"uint256"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_id","type":"uint256"},{"name":"_currentValue","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_msgSender","type":"address"}],"name":"approveAdapter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_currentValues","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"batchApprove","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_operator","type":"address"},{"name":"_ids","type":"uint256[]"},{"name":"_approved","type":"bool"}],"name":"setApproval","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_operator","type":"address"},{"name":"_approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_operator","type":"address"},{"name":"_id","type":"uint256"},{"name":"_approved","type":"bool"},{"name":"_msgSender","type":"address"}],"name":"setApprovalAdapter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_operator","type":"address"},{"name":"_id","type":"uint256"}],"name":"isApproved","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"},{"name":"_from","type":"address"},{"name":"_to","type":"address"}],"name":"transferFees","outputs":[{"name":"_transferValue","type":"uint256"},{"name":"_minTransferValue","type":"uint256"},{"name":"_transferFeeCurrency","type":"uint256"},{"name":"_fee","type":"uint256"},{"name":"_maxFee","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_askingIds","type":"uint256[]"},{"name":"_askingValues","type":"uint128[]"},{"name":"_offeringIds","type":"uint256[]"},{"name":"_offeringValues","type":"uint128[]"},{"name":"_secondParty","type":"address"}],"name":"createTrade","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"tradeCompletable","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"}],"name":"completeTrade","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"}],"name":"cancelTrade","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_ids","type":"uint256[]"},{"name":"_values","type":"uint256[]"}],"name":"melt","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_id","type":"uint256"},{"name":"_uri","type":"string"}],"name":"setURI","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"uri","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"nonFungibleCount","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_index","type":"uint256"}],"name":"nonFungibleByIndex","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"},{"name":"_owner","type":"address"},{"name":"_index","type":"uint256"}],"name":"nonFungibleOfOwnerByIndex","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_id","type":"uint256"}],"name":"isNonFungible","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":true,"inputs":[{"name":"_addr","type":"address"}],"name":"isContract","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_erc20ContractAddress","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"releaseERC20","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"releaseETH","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_erc721ContractAddress","type":"address"},{"name":"_to","type":"address"},{"name":"_token","type":"uint256"}],"name":"releaseERC721","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_erc1155ContractAddress","type":"address"},{"name":"_to","type":"address"},{"name":"_id","type":"uint256"},{"name":"_value","type":"uint256"}],"name":"releaseERC1155","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_storage","type":"address"},{"name":"_oldContract","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_nextContract","type":"address"}],"name":"retire","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}]
//const contract = new web3.eth.Contract(abi, '0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab');

let startBlockNumber = parseInt(fs.readFileSync('StartBlock.txt', 'utf8')) - 1;
var lastBlockNumber = startBlockNumber;

connectToDBB(client)

function connectToDBB(client) {
	client.connect( function (err, client) {
		if (err) throw err;

		startWatching();

	  }); 
};


/**
 * startWatching() is the top level function to begin
 * the contract event watching
 */
async function startWatching() {
	let watching = true;

	while (watching) {
		await watchEvents();
		wait(2500);
	}
}

/**
 * watchEvents() Provides top level logic to decide whether or 
 * not to update lastBlockNumber depending on the currentBlockNumber
 */

var tt = 0;
async function watchEvents() {
	let currBlockNumber = await getCurrBlockNumber();
	let latestCompleteBlock = currBlockNumber - 1;
	//let genBlock = 6043439; //ERC-1155 contract creation at txn:0x6e653115cacb3b8b226f6eff9234320c4e5f4e88e0988df2957a3bbca83ccb1b
	let blockInterval = 1000;
	
	//console.log(currBlockNumber, lastBlockNumber,blockInterval);	
	
	if ((currBlockNumber - lastBlockNumber) > blockInterval) {
		let start = (startBlockNumber + (tt * blockInterval)) + 1;
		let stop = (startBlockNumber + ((tt+1) * (blockInterval)));

		console.log("Getting events from: " + start + " to " + stop);
		let [eventsCreate, eventsMelt, eventsMint, eventsSetURI, eventsTransfer] = await checkBetweenBlocks(start, stop);
		lastBlockNumber = stop;
		tt++;

		//Write to database here
		eventsCreate.then(function(events) {
			var collection = "erc1155Events_create";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });
		
		eventsMelt.then(function(events) {
			var collection = "erc1155Events_melt";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsMint.then(function(events) {
			var collection = "erc1155Events_mint";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsSetURI.then(function(events) {
			var collection = "erc1155Events_setURI";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsTransfer.then(function(events) {
			var collection = "erc1155Events_transfer";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });
	}

	else if (lastBlockNumber < latestCompleteBlock) {
		let start = lastBlockNumber;
		let stop = latestCompleteBlock;
		
		console.log("Getting events from: " + start + " to " + stop);
		let [eventsCreate, eventsMelt, eventsMint, eventsSetURI, eventsTransfer] = await checkBetweenBlocks(start, stop);
		lastBlockNumber = currBlockNumber;

		eventsCreate.then(function(events) {
			var collection = "erc1155Events_create";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });
		
		eventsMelt.then(function(events) {
			var collection = "erc1155Events_melt";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsMint.then(function(events) {
			var collection = "erc1155Events_mint";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsSetURI.then(function(events) {
			var collection = "erc1155Events_setURI";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 });

		 eventsTransfer.then(function(events) {
			var collection = "erc1155Events_transfer";
			//console.log(events)
			updateDDBFromEvents(db,collection,events);
		 })		 

	} else {
		console.log("...watching the blockchain...")
	}
}

/**
 * updateDDBFromEvents() wil update DDB given event objects
 *
 * @param events {Array of Objects} Events to sync ddb with
 */
function updateDDBFromEvents(db,collection,events) {
	let counter = 0;
	var db = client.db('mzkz');
	//let petShopTable = 'pet-shop'
	
	for (let i=0; i<events.length; i++) {
		let eventObj = events[i];
		db.collection(collection).insertOne(eventObj);
		counter++;
	}
		//perform actions on the collection object
		
		// for (let i=0; i<events.length; i++) {
		// 	let eventObj = events[i];
	
		// 	// let params = {
		// 	//   TableName: petShopTable,
		// 	//   Key: {
		// 	// 	'id': parseInt(eventObj.returnValues.petId),
		// 	//   },
		// 	//   UpdateExpression: 'set ownerAddress = :o',
		// 	//   ExpressionAttributeValues: {
		// 	// 	':o' : eventObj.returnValues.owner.toString()
		// 	//   }
		// 	// };
	
		// 	// collection.update(eventObj, function(err, data) {
		// 	//   if (err) {
		// 	// 	console.log("Error", err);
		// 	//   }
		// 	// });
	
		// 	counter++;
		// }

	console.log("Made " + counter + " update(s) from " + events.length + db + " event(s)");
}


function readEventsFromDBB(db) {
	  
	var db = client.db('mzkz');
	
	db.collection('erc1155').findOne({}, function (findErr, result) {
		if (findErr) throw findErr;
		console.log(result);
	});
};



/**
 * getCurrBlockNumber() uses web3 to fetch the latest 
 * block number
 *
 * @return {Int} Current Ethereum block number
 */
function getCurrBlockNumber() {
	let currBlockNumber = web3.eth.getBlockNumber(function(error, result) {
		return result;
	});

	return currBlockNumber;
}

/**
 * checkBetweenBlocks() looks for events between two blocks and
 * returns them
 *
 * @param fromBlock {Int} Beginning block 
 * @param toBlock {Int} End block 
 * @return {Array of Objects} All found events 
 */
function checkBetweenBlocks(fromBlock, toBlock) {
	fs.writeFile('LastBlock.txt', fromBlock, (err) => {
	  if (err) throw err;
	});

	let eventsCreate= web3.eth.getPastLogs({
		fromBlock: fromBlock,
		toBlock: toBlock,
		address: "0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab",
		topics: ["0x250ed6814ddcc5fc06eec40c015c413d3aa7bfc4e1df91ed205e0d71f0a9408f"]
		}, function (error, eventsCreate) {
			//console.log("Checked for Create events between blocks");
			return eventsCreate;
		});	
	wait(100);	
	let eventsMelt = web3.eth.getPastLogs({
		fromBlock: fromBlock,
		toBlock: toBlock,
		address: "0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab",
		topics: ["0xba6a480970167b03ed2f35b55c48a436cd01efe96abdf846d1a64da47df0e6d9"]
		}, function (error, eventsMelt) {
			//console.log("Checked for Melt events between blocks");
			return eventsMelt;
		});	
	wait(100);
	let eventsMint = web3.eth.getPastLogs({
		fromBlock: fromBlock,
		toBlock: toBlock,
		address: "0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab",
		topics: ["0xcc9c58b575eabd3f6a1ee653e91fcea3ff546867ffc3782a3bbca1f9b6dbb8df"]
		}, function (error, eventsMint) {
			//console.log("Checked for Mint events between blocks");
			return eventsMint;
		});
	wait(100);
	let eventsSetURI = web3.eth.getPastLogs({
		fromBlock: fromBlock,
		toBlock: toBlock,
		address: "0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab",
		topics: ["0xee1bb82f380189104b74a7647d26f2f35679780e816626ffcaec7cafb7288e46"]
		}, function (error, eventsSetURI) {
			//console.log("Checked for SetURI events between blocks");
			return eventsSetURI;
		});	
	wait(100);
	let eventsTransfer = web3.eth.getPastLogs({
		fromBlock: fromBlock,
		toBlock: toBlock,
		address: "0x8562c38485B1E8cCd82E44F89823dA76C98eb0Ab",
		topics: ["0xf2dbd98d79f00f7aff338b824931d607bfcc63d47307162470f25a055102d3b0"]
		}, function (error, eventsTransfer) {
			//console.log("Checked for Transfer events between blocks");
			return eventsTransfer;
		});		

	return [eventsCreate, eventsMelt, eventsMint, eventsSetURI, eventsTransfer];
}
	
/**
 * wait() will consecutively wait corresponding time given
 * rather than putting the code to sleep
 *
 * @param ms {Int} Amount of ms to wait
 */
function wait(ms){
  	var start = new Date().getTime();
   	var end = start;

   	while(end < start + ms) {
    	end = new Date().getTime();
  	}
}

