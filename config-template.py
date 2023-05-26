## MySQL access info. Replace uppercase strings as indicated
passphrase = 'SQLPASSPHRASE'
dbstring = '%s:%s@%s/%s' % ('SQLUSER', 'SQLHOST', 'SQLDB', passphrase)

## Neo4J access info. Include if necessary, replace uppercase strings as indicated
graphuri = 'neo4j://%s:7687' % 'NEO4JHOST'
graphuser = 'neo4j' # Change this if necessary!
graphpw = 'NEO4JPASSPHRASE'
