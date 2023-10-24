Server assigns id:

- Changes when client reconnects

Client assigns id:

- What to do with client before :catch-up message?
- Where to store send-fn?
- Can track how far each client is. Why?


```
tx :: {:tx-data    [...]
       :tx-id      <any>
       :server-idx <long>}
```

# Client connects to a server

```
SND {:message    :catching-up
     :patterns   [<pattern> ...]
     :server-idx <long>?}

RCV {:message    :catched-up
     :snapshot   <serializable db>
     :server-idx <long>}

or

RCV {:message :catched-up
     :txs     [<tx> ...]}
```

# Client makes a transaction

```
SND {:message    :transacting
     :server-idx server-idx
     :txs        [{:tx-data ...
                   :tx-id   ...} ...]}
```

# Server broadcasts a transaction

```
RCV {:message    :transacted
     :tx-data    ...
     :tx-id      ...
     :server-idx ...} ...]}
```
