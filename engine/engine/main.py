import asyncio
import argparse
import uuid
from engine.connectors.singlestore import SingleStoreConnector
from engine.services.job import Job, JobService
from engine.config.config import Config

def parse_args():
    parser = argparse.ArgumentParser(description='Data migration CLI tool')
    
    # Source configuration
    source_group = parser.add_argument_group('Source configuration')
    source_group.add_argument('--src-engine', required=True, help='Source database engine')
    source_group.add_argument('--src-host', required=True, help='Source database host')
    source_group.add_argument('--src-port', type=int, required=True, help='Source database port')
    source_group.add_argument('--src-user', required=True, help='Source database username')
    source_group.add_argument('--src-password', required=True, help='Source database password')
    source_group.add_argument('--src-database', required=True, help='Source database name')
    source_group.add_argument('--src-table', required=True, help='Source table name')

    # Destination configuration
    dest_group = parser.add_argument_group('Destination configuration')
    dest_group.add_argument('--dst-engine', required=True, help='Destination database engine')
    dest_group.add_argument('--dst-host', required=True, help='Destination database host')
    dest_group.add_argument('--dst-port', type=int, required=True, help='Destination database port')
    dest_group.add_argument('--dst-user', required=True, help='Destination database username')
    dest_group.add_argument('--dst-password', required=True, help='Destination database password')
    dest_group.add_argument('--dst-database', required=True, help='Destination database name')
    dest_group.add_argument('--dst-table', required=True, help='Destination table name')

    # S3 configuration
    s3_group = parser.add_argument_group('S3 configuration')
    s3_group.add_argument('--s3-bucket', required=False, help='S3 bucket name')
    s3_group.add_argument('--s3-access-key-id', required=False, help='S3 access key ID')
    s3_group.add_argument('--s3-secret-access-key', required=False, help='S3 secret access key')

    # Other configuration
    parser.add_argument('-j', '--job-id', required=False, help='Job ID')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-m', '--migrate-only', action='store_true', help='Only migrate data, do not backup')
    parser.add_argument('-d', '--local-dir', required=False, help='Local directory to store data')
    parser.add_argument('-so', '--src-start-offset', type=int, required=False, help='Source start offset')
    parser.add_argument('-eo', '--src-end-offset', type=int, required=False, help='Source end offset')
    parser.add_argument('-sc', '--src-sort-column', required=False, help='Source sort column')
    parser.add_argument('-kd', '--keep-dest-table', action='store_true', help='Do not delete destination table before migration')

    args = parser.parse_args()
    
    # Validate required arguments
    required_args = [
        'src_engine', 'src_host', 'src_user', 'src_password', 'src_database', 'src_table',
        'dst_engine', 'dst_host', 'dst_user', 'dst_password', 'dst_database', 'dst_table',
    ]
    
    missing_args = [arg for arg in required_args if not getattr(args, arg)]
    if missing_args:
        parser.error(f"Missing required arguments: {', '.join(missing_args)}")

    # Check if S3 config is provided
    if args.s3_bucket and args.s3_access_key_id and args.s3_secret_access_key:
        Config.use_s3 = True
    else:
        Config.use_s3 = False
        if args.local_dir:
            Config.local_dir = args.local_dir
        
    if args.verbose:
        Config.verbose = True
        
    if args.migrate_only:
        Config.migrate_only = True

    if args.keep_dest_table:
        Config.reset_dest_table = False

    if args.job_id:
        Config.job_id = args.job_id
    else:
        Config.job_id = str(uuid.uuid4())
        
    return args

async def run_job(args):
    job = Job(
        job_id=Config.job_id,
        source_engine=args.src_engine,
        source_host=args.src_host,
        source_port=args.src_port,
        source_user=args.src_user,
        source_password=args.src_password,
        source_database=args.src_database,
        source_table=args.src_table,
        dest_engine=args.dst_engine,
        dest_host=args.dst_host,
        dest_port=args.dst_port,
        dest_user=args.dst_user,
        dest_password=args.dst_password,
        dest_database=args.dst_database,
        dest_table=args.dst_table,
        s3_bucket=args.s3_bucket,
        s3_access_key_id=args.s3_access_key_id,
        s3_secret_access_key=args.s3_secret_access_key,
        start_offset=args.src_start_offset,
        end_offset=args.src_end_offset,
        sort_column=args.src_sort_column
    )
    job_service = JobService(job)
    await job_service.run_job()

def main():
    """
    Run the main application logic with CLI arguments.
    """
    args = parse_args()
    asyncio.run(run_job(args))

if __name__ == "__main__":
    main()