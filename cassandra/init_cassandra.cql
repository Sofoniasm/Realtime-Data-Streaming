CREATE KEYSPACE IF NOT EXISTS events_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS events_keyspace.events (
  id uuid PRIMARY KEY,
  first_name text,
  last_name text,
  gender text,
  address text,
  post_code text,
  email text,
  username text,
  dob text,
  registered_date text,
  phone text,
  picture text
);
