
class DatabaseCollections {
	private Map<String, Database> databases; // Map of database names to database objects
	private String adminUser; // The pre-determined DB admin

	public DatabaseCollections(String adminUser) {
		databases = new HashMap<>();
		this.adminUser = adminUser;
	}

	// Create a new database and add it to the collection
	public void createDatabase(String databaseName) {
		databases.put(databaseName, new Database(databaseName));
	}

	// Get a database by its name
	public Database getDatabase(String databaseName) {
		return databases.get(databaseName);
	}

	// List all databases in the collection
	public Map<String, Database> listDatabases() {
		return databases;
	}

	// Remove a database from the collection
	public void dropDatabase(String databaseName) {
		databases.remove(databaseName);
	}

	// Get the admin user
	public String getAdminUser() {
		return adminUser;
	}
}




class Database {
	private String name; // Name of the database
	private TreeMap<String, Collection> collections; // Map of collection names to collection objects
	private TreeMap<String, Map<Object, Set<String>>> collectionIndexes; // Indexes for collections

	public Database(String name) {
		this.name = name;
		collections = new TreeMap<>();
		collectionIndexes = new TreeMap<>();
	}

	public String getName() {
		return name;
	}

	// Create a new collection and add it to the database
	public void createCollection(String collectionName) {
		collections.put(collectionName, new Collection(collectionName));

		// Initialize the index for this collection
		collectionIndexes.put(collectionName, new TreeMap<>());
	}

	// Get a collection by its name
	public Collection getCollection(String collectionName) {
		return collections.get(collectionName);
	}

	// List all collections in the database
	public Map<String, Collection> listCollections() {
		return collections;
	}

	// Remove a collection from the database
	public void dropCollection(String collectionName) {
		collections.remove(collectionName);

		// Remove the index for this collection
		collectionIndexes.remove(collectionName);
	}

	@Override
	public String toString() {
		return "Database Name: " + name + ", Number of Collections: " + collections.size();
	}
}



class Collection<T>  {
	private String name; // Name of the collection
	private Map<String, Document<T>> documents; // Collection of documents

	// Maintain a map of indexes for efficient property-based queries
	private TreeMap<String, Map<T, Set<String>>> propertyIndexes;

	public Collection(String name) {
		this.name = name;
		documents = new TreeMap<>();
		propertyIndexes = new TreeMap<>();
	}

	public String getName() {
		return name;
	}

	// Create a new document and add it to the collection
	public void createDocument(Document<T> document) {
		documents.put(document.getId(), document);

		// Update property indexes for the new document
		updateIndexes(document);
	}

	// Delete a document from the collection
	public void deleteDocument(String documentId) {
		Document<T> documentToRemove = documents.get(documentId);
		if (documentToRemove != null) {
			// Remove the document from the collection
			documents.remove(documentId);

			// Remove entries from property indexes
			removeFromIndexes(documentToRemove);
		}
	}

	// Get a document by its ID
	public Document<T> getDocumentById(String documentId) {
		return documents.get(documentId);
	}

	// List all documents in the collection
	public List<Document<T>> listDocuments() {
		return new ArrayList<>(documents.values());
	}

	// Update property indexes for a document
	private void updateIndexes(Document<T> document) {
		for (Map.Entry<String, T> entry : document.getProperties().entrySet()) {
			String propertyName = entry.getKey();
			T propertyValue = entry.getValue();

			// Ensure the property index exists for this property
			Map<T, Set<String>> index = propertyIndexes.computeIfAbsent(propertyName, k -> new HashMap<>());

			if (propertyValue instanceof Iterable<?>) {
				// Handle lists and arrays
				for (T item : (Iterable<T>) propertyValue) {
					index.computeIfAbsent(item, k -> new HashSet<>()).add(document.getId());
				}
			} else {
				// Handle single values
				Set<String> documentIds = index.get(propertyValue);
				if (documentIds == null) {
					documentIds = new HashSet<>();
					index.put(propertyValue, documentIds);
				}
				documentIds.add(document.getId());
			}
		}
	}



	// Remove entries from property indexes for a document
	private void removeFromIndexes(Document<T> document) {
		for (Map.Entry<String, T> entry : document.getProperties().entrySet()) {
			String propertyName = entry.getKey();
			T propertyValue = entry.getValue();

			propertyIndexes.computeIfPresent(propertyName, (key, index) -> {
				Set<String> documentIds = index.get(propertyValue);
				if (documentIds != null) {
					documentIds.remove(document.getId());
					if (documentIds.isEmpty()) {
						index.remove(propertyValue);
					}
				}
				return index;
			});
		}
	}



	// Query documents based on a property and its value
	public List<Document<T>> queryByProperty(String propertyName, T value) {
		Map<T, Set<String>> propertyIndex = propertyIndexes.get(propertyName);
		if (propertyIndex == null) {
			return Collections.emptyList(); // Property not found
		}

		Set<String> documentIds = propertyIndex.get(value);
		if (documentIds == null) {
			return Collections.emptyList(); // Value not found
		}

		List<Document<T>> result = new ArrayList<>(documentIds.size());
		for (String documentId : documentIds) {
			Document<T> document = getDocumentById(documentId);
			if (document != null) {
				result.add(document);
			}
		}

		return result;
	}



	@Override
	public String toString() {
		return "Collection Name: " + name + ", Number of Documents: " + documents.size();
	}
}






class Document<T> {
	private final String id;
	private final Map<String, T> properties;
	private final Map<String, Map<T, Set<String>>> propertyIndex;

	public Document() {
		id = generateUniqueId();
		properties = new TreeMap<>();
		propertyIndex = new TreeMap<>();
	}

	private String generateUniqueId() {
		return System.nanoTime() + "-" + UUID.randomUUID();
	}

	public String getId() {
		return id;
	}

	public void setProperty(String key, T value) {
		properties.put(key, value);

		if (value instanceof Iterable<?>) {
			// Handle lists and arrays
			Iterable<T> iterableValue = (Iterable<T>) value;
			Map<T, Set<String>> index = propertyIndex.computeIfAbsent(key, k -> new HashMap<>());

			for (T item : iterableValue) {
				index.computeIfAbsent(item, k -> new HashSet<>()).add(id);
			}
		} else {
			// Handle single values
			propertyIndex.computeIfAbsent(key, k -> new HashMap<>())
					.computeIfAbsent(value, k -> new HashSet<>())
					.add(id);
		}
	}




	public Map<String, T> getProperties() {
		return properties;
	}

	public T getProperty(String key) {
		return properties.get(key);
	}

	public String searchByProperty(String propertyName, T value) {
		Map<T, Set<String>> index = propertyIndex.get(propertyName);
		if (index != null) {
			Set<String> documentIds = index.get(value);
			if (documentIds != null && !documentIds.isEmpty()) {
				return documentIds.iterator().next();
			}
		}
		return null;
	}

	public void updateProperty(String key, T newValue) {
		if (properties.containsKey(key)) {
			T oldValue = properties.put(key, newValue);

			if (!oldValue.equals(newValue)) {
				Map<T, Set<String>> index = propertyIndex.get(key);

				if (index != null) {
					Set<String> oldIds = index.get(oldValue);
					if (oldIds != null) {
						oldIds.remove(id);
						if (oldIds.isEmpty()) {
							index.remove(oldValue);
						}
					}

					index.computeIfAbsent(newValue, k -> new HashSet<>()).add(id);
				}
			}
		}
	}



	public void removeProperty(String key) {
		if (properties.containsKey(key)) {
			T valueToRemove = properties.remove(key);

			Map<T, Set<String>> index = propertyIndex.get(key);
			if (index != null) {
				Set<String> documentIds = index.get(valueToRemove);
				if (documentIds != null) {
					documentIds.remove(id);
					if (documentIds.isEmpty()) {
						index.remove(valueToRemove);
					}
				}
			}
		}
	}

	public void createIndexForProperty(String propertyName) {
		propertyIndex.computeIfAbsent(propertyName, k -> new TreeMap<>());
	}

	public void addToPropertyIndex(String propertyName, T value) {
		propertyIndex
				.computeIfAbsent(propertyName, k -> new TreeMap<>())
				.computeIfAbsent(value, k -> new HashSet<>())
				.add(id);
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder("Document ID: ").append(id).append(", Properties: {");

		for (Map.Entry<String, T> entry : properties.entrySet()) {
			String propertyName = entry.getKey();
			T propertyValue = entry.getValue();

			stringBuilder.append(propertyName).append("=");

			if (propertyValue.getClass().isArray()) {
				// Handle arrays
				stringBuilder.append("[");
				boolean first = true;
				for (int i = 0; i < Array.getLength(propertyValue); i++) {
					Object item = Array.get(propertyValue, i);
					if (!first) {
						stringBuilder.append(", ");
					}
					stringBuilder.append(item);
					first = false;
				}
				stringBuilder.append("]");
			} else if (propertyValue instanceof Iterable<?>) {
				// Handle Iterable (e.g., lists)
				stringBuilder.append("[");
				boolean first = true;
				for (Object item : (Iterable<?>) propertyValue) {
					if (!first) {
						stringBuilder.append(", ");
					}
					stringBuilder.append(item);
					first = false;
				}
				stringBuilder.append("]");
			} else {
				// Handle single values
				stringBuilder.append(propertyValue);
			}

			stringBuilder.append(", ");
		}

		// Remove the trailing comma and space
		if (properties.size() > 0) {
			stringBuilder.setLength(stringBuilder.length() - 2);
		}

		stringBuilder.append("}");

		return stringBuilder.toString();
	}

}



public class Main {
	public static void main(String[] args) {
		DatabaseCollections databaseCollections = new DatabaseCollections("admin");

		// Create a database named "mydb"
		databaseCollections.createDatabase("mydb");

		// Get the "mydb" database
		Database myDatabase = databaseCollections.getDatabase("mydb");

		// Create a collection named "persons"
		myDatabase.createCollection("persons");
		Collection<Object> persons = myDatabase.getCollection("persons");

		// Measure the time for each operation
		long startTime, endTime, elapsedTime;

		// Create and insert documents with different property types
		Document<Object> person1 = new Document<>();
		startTime = System.nanoTime();
		person1.setProperty("name", "John");
		endTime = System.nanoTime();
		double executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for  inserting person1: " + executionTimeInMillis + " ms");
		person1.setProperty("age", 30);
		person1.setProperty("height", 180.5);
		person1.setProperty("isStudent", false);
		person1.setProperty("hobbies", new String[]{"Reading", "Hiking"});
		startTime = System.nanoTime();
		persons.createDocument(person1);
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for creating person1: " + executionTimeInMillis + " ms");

//		for (int i=0 ; i < 10000 ; i++)
//		{
//			Document<Object> person5 = new Document<>();
//			person5.setProperty("name", "Alice");
//			person5.setProperty("age", 25);
//			person5.setProperty("height", 165.0);
//			person5.setProperty("isStudent", true);
//			person5.setProperty("hobbies", new String[]{"Swimming", "Cooking"});
//			person5.setProperty("courses", Arrays.asList("Math", "English"));
//			persons.createDocument(person5);
//		}

		Document<Object> person2 = new Document<>();
		startTime = System.nanoTime();
		person2.setProperty("name", "Alice");
		person2.setProperty("age", 25);
		person2.setProperty("height", 165.0);
		person2.setProperty("isStudent", true);
		person2.setProperty("hobbies", new String[]{"Swimming", "Cooking"});
		person2.setProperty("courses", Arrays.asList("Math", "English"));
		persons.createDocument(person2);
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for creating and inserting person2: " + executionTimeInMillis + " ms");

		// Query documents based on properties
		startTime = System.nanoTime();
		List<Document<Object>> students = persons.queryByProperty("isStudent", true);
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for querying students: " + executionTimeInMillis + " ms");
		System.out.println("Students:");
		for (Document<Object> student : students) {
			System.out.println(student);
		}

		// Update a property of a document
		startTime = System.nanoTime();
		Document<Object> updatedPerson = persons.getDocumentById(person1.getId());
		updatedPerson.updateProperty("age", 31);
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for updating person1's age: " + executionTimeInMillis + " ms");
		System.out.println("Updated Person:");
		System.out.println(updatedPerson);

		// Delete a document
		startTime = System.nanoTime();
		persons.deleteDocument(person2.getId());
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for deleting person2: " + executionTimeInMillis + " ms");
		System.out.println("Remaining Persons:");
		for (Document<Object> remainingPerson : persons.listDocuments()) {
			System.out.println(remainingPerson);
		}

		// Drop the "persons" collection
		startTime = System.nanoTime();
		myDatabase.dropCollection("persons");
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for dropping the 'persons' collection: " + executionTimeInMillis + " ms");

		// Drop the "mydb" database
		startTime = System.nanoTime();
		databaseCollections.dropDatabase("mydb");
		endTime = System.nanoTime();
		executionTimeInMillis = (endTime - startTime) / 1e6; // Convert to milliseconds
		System.out.println("Time for dropping the 'mydb' database: " + executionTimeInMillis + " ms");
	}
}

