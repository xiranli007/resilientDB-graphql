from resdb_driver import Resdb
from resdb_driver.crypto import generate_keypair
import ast

db_root_url = "localhost:18000"
protocol = "http://"
fetch_all_endpoint = "/v1/transactions"
db = Resdb(db_root_url)

import strawberry
import typing
from typing import Optional, List
from filter import filter_by_keys

from flask import Flask
from flask_cors import CORS
from datetime import datetime
import uuid

app = Flask(__name__)
CORS(app) # This will enable CORS for all routes

from strawberry.flask.views import GraphQLView

@strawberry.type
class RetrieveTransaction:
    id: str
    version: str
    amount: int
    uri: str
    type: str
    publicKey: str
    operation: str
    metadata: typing.Optional["str"]
    asset: str

@strawberry.type
class CommitTransaction:
    id: str

@strawberry.input
class PrepareAsset:
    operation: str
    amount: int
    signerPublicKey: str
    signerPrivateKey: str
    recipientPublicKey: str
    asset: str

@strawberry.input
class UpdateAsset:
    id: str
    operation: typing.Optional["str"]
    amount: typing.Optional["int"]
    signerPublicKey: str
    signerPrivateKey: str
    recipientPublicKey: typing.Optional["str"]
    asset: typing.Optional["str"]

@strawberry.input
class FilterKeys:
    ownerPublicKey: Optional[str]
    recipientPublicKey: Optional[str]

@strawberry.type
class Keys:
    publicKey: str
    privateKey: str

def update(data):
    record = db.transactions.retrieve(data.id)
    prepared_token_tx = db.transactions.prepare(
    operation=record["operation"] if data.operation == "" else data.operation,
    signers=data.signerPublicKey,
    recipients=[([record["outputs"][0]["condition"]["details"]["public_key"] if data.recipientPublicKey == "" else data.recipientPublicKey], record["outputs"][0]["amount"] if data.amount == "" else data.amount)],
    asset=record["asset"] if data.asset == "" else ast.literal_eval(data.asset),
    )

    # fulfill the tnx
    fulfilled_token_tx = db.transactions.fulfill(prepared_token_tx, private_keys=data.signerPrivateKey)

    id = db.transactions.send_commit(fulfilled_token_tx)[4:] # Extract ID
    data = db.transactions.retrieve(txid=id)
    payload = RetrieveTransaction(
        id=data["id"],
        version=data["version"],
        amount=data["outputs"][0]["amount"],
        uri=data["outputs"][0]["condition"]["uri"],
        type=data["outputs"][0]["condition"]["details"]["type"],
        publicKey=data["outputs"][0]["condition"]["details"]["public_key"],
        operation=data["operation"],
        metadata=data["metadata"],
        asset=str(data["asset"])
    )
    return payload

@strawberry.type
class Query:
    @strawberry.field
    def getTransaction(self, id: strawberry.ID) -> RetrieveTransaction:
        data = db.transactions.retrieve(txid=id)
        payload = RetrieveTransaction(
            id=data["id"],
            version=data["version"],
            amount=data["outputs"][0]["amount"],
            uri=data["outputs"][0]["condition"]["uri"],
            type=data["outputs"][0]["condition"]["details"]["type"],
            publicKey=data["outputs"][0]["condition"]["details"]["public_key"],
            operation=data["operation"],
            metadata=data["metadata"],
            asset=str(data["asset"])
        )
        return payload
    
    @strawberry.field
    def getFilteredTransactions(self, filter: Optional[FilterKeys]) -> List[RetrieveTransaction]:
        url = f"{protocol}{db_root_url}{fetch_all_endpoint}"
        if filter.ownerPublicKey != None:
            filter.ownerPublicKey = filter.ownerPublicKey if filter.ownerPublicKey.strip() else None
        if filter.recipientPublicKey != None:
            filter.recipientPublicKey = filter.recipientPublicKey if filter.recipientPublicKey.strip() else None
        json_data = filter_by_keys(url, filter.ownerPublicKey, filter.recipientPublicKey)
        records = []
        for data in json_data:
            try:
                records.append(RetrieveTransaction(
                id=data["id"],
                version=data["version"],
                amount=data["outputs"][0]["amount"],
                uri=data["outputs"][0]["condition"]["uri"],
                type=data["outputs"][0]["condition"]["details"]["type"],
                publicKey=data["outputs"][0]["condition"]["details"]["public_key"],
                operation=data["operation"],
                metadata=data["metadata"],
                asset=str(data["asset"])
                ))
            except Exception as e:
                print(e)
        return records
    
    @strawberry.field
    def getSpecificDataStructure(self, filter: Optional[FilterKeys], required_keys: List[str]) -> List[RetrieveTransaction]:
        query = Query()
        filter_keys = FilterKeys(ownerPublicKey="", recipientPublicKey="")
        records = query.getFilteredTransactions(filter_keys)  # Fetch all transaction
        valid_records = []

        for record in records:
            try:
                print(f"Raw asset: {record.asset}")

                if not record.asset:
                    print(f"Record {record.id} has no asset. Skipping.")
                    continue

                asset = ast.literal_eval(record.asset)

                if "data" in asset:
                    if all(key in asset["data"] for key in required_keys):
                        valid_records.append(record)
                    else:
                        print(f"Missing required keys in 'data': {asset['data']}")
                else:
                    print(f"'data' key missing in asset: {asset}")

            except Exception as e:
                print(f"Error parsing asset for record {record.id}: {e}")

        return valid_records

    @strawberry.field
    def getTransactionsByElectionId(
        self, filter: Optional[FilterKeys], electionId: str
    ) -> List[RetrieveTransaction]:
        query = Query()
        filter_keys = FilterKeys(ownerPublicKey="", recipientPublicKey="")
        records = query.getFilteredTransactions(filter_keys)  # Fetch all transactions
        matching_records = []

        for record in records:
            try:
                asset = ast.literal_eval(record.asset)

                # Check if 'data' key exi
                if "data" in asset:
                    data = asset["data"]

                    # Check if 'currentElectionId' field exists
                    if "currentElectionId" in data:
                        # Match the currentElectionId field with the provided election_id
                        if data["currentElectionId"] == electionId:
                            matching_records.append(record)
                    else:
                        print(f"Transaction {record.id} does not have 'currentElectionId'. Skipping.")
                else:
                    print(f"'data' key missing in asset: {asset}")

            except Exception as e:
                print(f"Error parsing asset for record {record.id}: {e}")

        return matching_records


    @strawberry.field
    def fetchElectionById(self, id: str) -> RetrieveTransaction:
        # Logic for retrieving election data
        transaction = db.transactions.retrieve(txid=id)
        asset = transaction.get("asset")
        return RetrieveTransaction(
            id=transaction.get("id"),
            version=transaction.get("version"),
            amount=transaction["outputs"][0]["amount"],
            uri=transaction["outputs"][0]["condition"]["uri"],
            type=transaction["outputs"][0]["condition"]["details"]["type"],
            publicKey=transaction["outputs"][0]["condition"]["details"]["public_key"],
            operation=transaction["operation"],
            metadata=transaction.get("metadata"),
            asset=str(asset),
        )


@strawberry.type
class Mutation:
    @strawberry.mutation
    def postTransaction(self, data: PrepareAsset) -> CommitTransaction:
        prepared_token_tx = db.transactions.prepare(
            operation=data.operation,
            signers=data.signerPublicKey,
            recipients=[([data.recipientPublicKey], data.amount)],
            asset=ast.literal_eval(data.asset),
        )

        # fulfill the tnx
        fulfilled_token_tx = db.transactions.fulfill(prepared_token_tx, private_keys=data.signerPrivateKey)
        id = db.transactions.send_commit(fulfilled_token_tx)[4:] # Extract ID

    
        payload = CommitTransaction(
            id=id,
        )
        return payload
    
    @strawberry.mutation
    def updateTransaction(self, data: UpdateAsset) -> RetrieveTransaction:
        return update(data)
    
    @strawberry.mutation
    def updateMultipleTransaction(self, data: List[UpdateAsset]) -> List[RetrieveTransaction]:
        result = []
        for transaction in data:
            result.append(update(transaction))
        return result
    
    

    @strawberry.mutation
    def generateKeys(self) -> Keys:
        keys = generate_keypair()
        payload = Keys(
            publicKey=keys.public_key,
            privateKey=keys.private_key
        )
        return payload
    
    

schema = strawberry.Schema(query=Query, mutation=Mutation)

app.add_url_rule(
    "/graphql",
    view_func=GraphQLView.as_view("graphql_view", schema=schema),
)

if __name__ == "__main__":
    app.run(port="8000")
