package be.vlaanderen.informatievlaanderen.ldes.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.base.AbstractIRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "ldes-triplestore, vsds" })
@CapabilityDescription("Materialises LDES events in a triplestore (SPARQL query)")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class LdesTriplestoreMaterialisation extends AbstractProcessor {
	private RepositoryManager repositoryManager;

	static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("A FlowFile is routed to this relationship after the database is successfully updated")
			.build();
	static final Relationship REL_RETRY = new Relationship.Builder()
			.name("retry")
			.description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
			.build();
	static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
					+ "such as an invalid query or an integrity constraint violation")
			.build();

	static final PropertyDescriptor SPARQL_HOST = new PropertyDescriptor.Builder()
			.name("SPARQL host")
			.description("The hostname and port of the SPARQL server.")
			.defaultValue("http://graphdb:7200")
			.required(true)
			.addValidator(StandardValidators.URL_VALIDATOR)
			.build();

	static final PropertyDescriptor REPOSITORY_ID = new PropertyDescriptor.Builder()
			.name("Repository ID")
			.description("The repository to connect to.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	static final PropertyDescriptor NAMED_GRAPH = new PropertyDescriptor.Builder()
			.name("Named graph")
			.description("If set, the named graph the triples will be written to.")
			.required(false)
			.addValidator(StandardValidators.URI_VALIDATOR)
			.build();


	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		this.repositoryManager = new RemoteRepositoryManager(context.getProperty(SPARQL_HOST).getValue());
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowfile = session.get();

		if(flowfile == null) {
			context.yield();
			return;
		}

		session.read(flowfile, new InputStreamCallback() {

			@Override
			public void process(InputStream in) throws IOException {
					String FragmentRDF = IOUtils.toString(in);
					getLogger().debug(String.format("Got the following RDF: %s", FragmentRDF));
					InputStream targetStream = IOUtils.toInputStream(FragmentRDF);
					Model updateModel = Rio.parse(targetStream, "", RDFFormat.TURTLE);

					Set<Resource> entityIds = getSubjectsFromModel(updateModel);

					Repository repository = repositoryManager.getRepository(context.getProperty(REPOSITORY_ID).getValue());
					try (RepositoryConnection dbConnection = repository.getConnection()) {


						// Start a transaction to avoid autocommit.
						dbConnection.begin();

						// Delete the old version of the entity from the db.
						deleteEntitiesFromRepo(entityIds, dbConnection);

						// Save the new data to the DB.
						String namedGraph = context.getProperty(NAMED_GRAPH).getValue();
						IRI namedGraphIRI = dbConnection.getValueFactory().createIRI(namedGraph);
						dbConnection.add(updateModel, namedGraphIRI);

						dbConnection.commit();
					}
			}
		});

		// To write the results back out ot flow file
		flowfile = session.write(flowfile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				//out.write(value.get().getBytes());
			}
		});

		session.transfer(flowfile, REL_SUCCESS);
	}

	/**
	 * Returns all subjects ('real' URIs) present in the model.
	 * @param model A graph
	 * @return A set of subject URIs.
	 */
	private static Set<Resource> getSubjectsFromModel(Model model) {
		Set<Resource> entityIds = new HashSet<>();
		model.subjects().forEach((Resource subject) -> {
			if (subject instanceof AbstractIRI) {
				entityIds.add(subject);
			}
		});
		return entityIds;
	}

	/**
	 * Delete an entity, including its blank nodes, from a repository.
	 * @param entityIds The subjects of the entities to delete.
	 * @param connection The DB connection.
	 */
	private static void deleteEntitiesFromRepo(Set<Resource> entityIds, RepositoryConnection connection) {
		Stack<Resource> subjectStack = new Stack<>();
		entityIds.forEach(subjectStack::push);
		/*
		 * Entities can contain blank node references. All statements with those blank node identifiers
		 * need to be removed as well. As blank nodes can be nested inside blank nodes, we need to keep track of them
		 * as they are encountered by adding them to the stack.
		 */
		while (!subjectStack.isEmpty()) {
			Resource subject = subjectStack.pop();
			connection.getStatements(subject, null, null).forEach((Statement statement) -> {
				Value object = statement.getObject();
				if (object.isBNode()) {
					subjectStack.push((Resource) object);
				}
			});
			connection.remove(subject, null, null);
		}
	}
	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_SUCCESS);
		rels.add(REL_RETRY);
		rels.add(REL_FAILURE);
		return rels;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(SPARQL_HOST);
		properties.add(REPOSITORY_ID);
		properties.add(NAMED_GRAPH);
		return properties;
	}
}
