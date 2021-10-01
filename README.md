# socket-dht

The purpose of this project is to implement your own application program in which processes communicate using sockets to maintain a distributed hash table (DHT) dynamically, and answer queries using it.

## Running this project

```bash
python ServerDriver.py 65432

# -- S = Server ; C = Client ; P = Port
python ClientDriver.py ⟨username⟩ ⟨S IP⟩ ⟨S P⟩ ⟨C IP⟩ ⟨C left P⟩ ⟨C query P⟩ ⟨C accept P⟩

# Once a client is running you can send 'help' to learn more about the commands available
# help
```

### Commands given to the clients

```bash
# Register users after starting the client script
register

# n is the number of users you want in the DHT
setup-dht {n}

# query the DHT after it is set up
query-dht

# Leave the DHT from one of the clients currently maintaining
leave-dht

# Query the DHT again after a user leaves
query-dht

# Join the DHT form a user not currently maintaining
join-dht

# Terminate the DHT when finished
terminate-dht

# deregister the user once they're free
deregister
```
