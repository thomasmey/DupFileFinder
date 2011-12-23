package jobcontrol;

public class Job {

	private JobCommon common;
	private Object[] mainMethodParameters;

	public Job(JobCommon common) {
		this.common = common;
	}

	public void setMainMethodParameters(Object... mainMethodParameters) {
		this.mainMethodParameters = mainMethodParameters;
	}

	public JobCommon getJobCommon() {
		return this.common;
	}

	public Object[] getMainMethodParameters() {
		return mainMethodParameters;
	}
}
