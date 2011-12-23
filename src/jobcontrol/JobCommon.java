package jobcontrol;

public class JobCommon {

	private Class    jobClass;
	private String   mainMethod;
	private Class[]  constructorParameterTypes;
	private Object[] constructorParameters;
	private Class[]  mainMethodParameterTypes;
	private boolean  poolable;

	public JobCommon(Class jobClass,
			Class[] constructorParameterTypes, Object[] constructorParameters,
			String mainMethod,
			Class[] mainMethodParameterTypes,
			boolean poolable) {
		super();
		this.jobClass = jobClass;
		this.mainMethod = mainMethod;
		this.constructorParameterTypes = constructorParameterTypes;
		this.constructorParameters = constructorParameters;
		this.mainMethodParameterTypes = mainMethodParameterTypes;
		this.poolable = poolable;
	}

	public Class getJobClass() {
		return jobClass;
	}

	public String getMainMethod() {
		return mainMethod;
	}

	public Class[] getConstructorParameterTypes() {
		return constructorParameterTypes;
	}

	public Object[] getConstructorParameters() {
		return constructorParameters;
	}

	public Class[] getMainMethodParameterTypes() {
		return mainMethodParameterTypes;
	}

	public boolean isPoolable() {
		return poolable;
	}
	
}
