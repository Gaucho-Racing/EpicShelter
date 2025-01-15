import asyncio
from engine.connectors.singlestore import SingleStoreConnector
from engine.services.job import Job, JobService

def main():
    """
    Run the main application logic.
    """
    asyncio.run(test_run())

async def test_run():
    job = Job(
        
    )

    job_service = JobService(job)
    await job_service.run_job()


if __name__ == "__main__":
    main()