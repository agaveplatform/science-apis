db = db.getSiblingDB("DATABASE");
db.metadata.find();
db.schemata.find();
db.createUser(
  {
    user: "USERNAME",
    pwd: "USERPASS",
    roles: [
      {
        role: "dbOwner",
        db: "api"
      },
      {
        role: "dbOwner",
        db: "notifications"
      }
    ]
  }
);
db = db.getSiblingDB('notifications');
db.createUser(
    {
        user: "USERNAME",
        pwd: "USERPASS",
        roles: [
            {
                role: "dbOwner",
                db: "notifications"
            }
        ]
    }
);
