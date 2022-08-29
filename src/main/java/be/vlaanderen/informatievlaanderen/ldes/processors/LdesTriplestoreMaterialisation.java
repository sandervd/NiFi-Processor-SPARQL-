package be.vlaanderen.informatievlaanderen.ldes.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessorInitializationContext;

@Tags({ "ldes-triplestore, vsds" })
@CapabilityDescription("Materialises LDES events in a triplestore (SPARQL query)")
@Stateful(description = "Stores mutable fragments to allow processor restart", scopes = Scope.LOCAL)
public class LdesTriplestoreMaterialisation extends AbstractProcessor {
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	private static final Logger LOGGER = LoggerFactory.getLogger(LdesTriplestoreMaterialisation.class);

	private RepositoryManager repositoryManager = new RemoteRepositoryManager("http://graphdb:7200");
	public static final PropertyDescriptor REPOSITORY_CONFIG = new PropertyDescriptor.Builder()
			.name("REPO_CONFIG")
			.displayName("Repository configuration")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor REPOSITORY_ID = new PropertyDescriptor.Builder()
			.name("REPO_ID")
			.displayName("Repository ID")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();


	public static final Relationship SUCCESS = new Relationship.Builder()
			.name("SUCCESS")
			.description("Succes relationship")
			.build();

	@Override
	public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
		if (descriptor.getName().equals("REPO_CONFIG")) {
			this.getLogger().debug("(Re-)loading repository config.");
			//SPARQLRepositoryConfig sparqlRepoConfig = new SPARQLRepositoryConfig();
			//sparqlRepoConfig.setQueryEndpointUrl("http://graphdb:7200/sparql");
			//sparqlRepoConfig.setUpdateEndpointUrl("http://graphdb:7200/sparql");
			//RepositoryConfig repConfig = new RepositoryConfig("sparql_repository", sparqlRepoConfig);
			//this.repositoryManager.addRepositoryConfig(repConfig);
		}
	}

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		//properties.add(REPOSITORY_CONFIG);
		//properties.add(REPOSITORY_ID);
		this.properties = Collections.unmodifiableList(properties);

		Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);

		// Repository manager
		this.repositoryManager.init();

		//throw new RuntimeException("Scheduling not implemented");

	}
	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final AtomicReference<String> value = new AtomicReference<>();
		FlowFile flowfile = session.get();

		if(flowfile == null) {
			context.yield();
			return;
		}

		Repository repository = this.repositoryManager.getRepository("vsds");

		session.read(flowfile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
					String FragmentRDF = IOUtils.toString(in);
					getLogger().debug(String.format("Got the following RDF: %s", FragmentRDF));
					try (RepositoryConnection con = repository.getConnection()) {
						InputStream targetStream = IOUtils.toInputStream(FragmentRDF);
						con.begin();
						con.add(targetStream, RDFFormat.N3);
						con.commit();
					}
					catch (RDF4JException e) {
						getLogger().error(e.getMessage());
						getLogger().error(e.getStackTrace().toString());
					}
			}
		});

		// Write the results to an attribute
		String results = value.get();
		if(results != null && !results.isEmpty()){
			flowfile = session.putAttribute(flowfile, "match", results);
		}

		// To write the results back out ot flow file
		flowfile = session.write(flowfile, new OutputStreamCallback() {

			@Override
			public void process(OutputStream out) throws IOException {
				//out.write(value.get().getBytes());
			}
		});

		session.transfer(flowfile, SUCCESS);
	}
	@Override
	public Set<Relationship> getRelationships(){
		return relationships;
	}

	@Override
	public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
		return properties;
	}
}
