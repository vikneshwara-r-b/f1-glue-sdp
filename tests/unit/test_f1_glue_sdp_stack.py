import aws_cdk as core
import aws_cdk.assertions as assertions

from f1_glue_sdp.f1_glue_sdp_stack import F1GlueSdpStack
from f1_glue_sdp.pipeline_stack_props import PipelineStackProps


def _synth_template() -> assertions.Template:
    app = core.App()
    props = PipelineStackProps(
        bucket_name="f1-glue-sdp-test",
        prefix="f1-pipeline",
        database_name="f1_sdp_db",
        job_name="f1-sdp-job",
    )
    stack = F1GlueSdpStack(app, "TestStack", props=props)
    return assertions.Template.from_stack(stack)


def test_bucket_is_encrypted_and_private():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )


def test_bucket_enforces_ssl():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Effect": "Deny",
                                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                            }
                        )
                    ]
                )
            }
        },
    )


def test_glue_role_trust_policy_and_managed_policy():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    assertions.Match.object_like(
                        {
                            "Principal": {"Service": "glue.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    )
                ]
            },
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "Fn::Join": assertions.Match.array_with(
                                [
                                    assertions.Match.array_with(
                                        [assertions.Match.string_like_regexp("AWSGlueServiceRole")]
                                    )
                                ]
                            )
                        }
                    )
                ]
            ),
        },
    )


def test_glue_role_inline_policy_is_scoped_not_wildcard():
    template = _synth_template()
    policies = template.find_resources("AWS::IAM::Policy")
    assert policies, "expected an inline IAM policy on the Glue role"

    for policy in policies.values():
        statements = policy["Properties"]["PolicyDocument"]["Statement"]
        for statement in statements:
            resource = statement.get("Resource")
            if resource is None:
                continue
            resources = resource if isinstance(resource, list) else [resource]
            for res in resources:
                assert res != "*", "S3 inline policy resource must not be '*'"


def test_glue_database_has_location_uri():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Glue::Database",
        {
            "DatabaseInput": {
                "Name": "f1_sdp_db",
                "LocationUri": "s3://f1-glue-sdp-test/f1-pipeline/warehouse/",
            }
        },
    )


def test_glue_job_uses_version_6_and_sdp_flags():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "Name": "f1-sdp-job",
            "GlueVersion": "6.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "DefaultArguments": {
                "--enable-spark-declarative-pipeline": "true",
                "--additional-python-modules": "requests==2.32.3",
            },
        },
    )


def test_stack_only_deploys_a_keep_marker_not_real_reference_data():
    # reference_data/ (repo root) itself is deliberately kept out of the CDK
    # stack -- a plain `cdk deploy` must never upload its real CSVs (see
    # scripts/deploy.sh's --upload-reference-data flag for that opt-in path).
    # Exactly one BucketDeployment is expected: a tiny `.keep` marker so
    # circuit_track_type's streaming read doesn't hit [PATH_NOT_FOUND] on a
    # brand-new deployment with no uploaded data yet.
    template = _synth_template()
    template.resource_count_is("Custom::CDKBucketDeployment", 1)
    template.has_resource_properties(
        "Custom::CDKBucketDeployment",
        {
            "DestinationBucketKeyPrefix": "f1-pipeline/reference-data",
            # Critical: must never prune:true, or this marker deployment would
            # delete real CSVs a prior --upload-reference-data run already
            # placed in this same prefix, on every single `cdk deploy`.
            "Prune": False,
        },
    )


def test_glue_job_does_not_enable_glue_datacatalog():
    # Iceberg tables in this pipeline use catalog-impl=GlueCatalog (see
    # pipeline_src/spark-pipeline.yml). AWS's SDP docs state that combining
    # --enable-glue-datacatalog with that configuration "fails on AWS Glue 6.0".
    template = _synth_template()
    jobs = template.find_resources("AWS::Glue::Job")
    assert jobs, "expected a Glue job resource"

    for job in jobs.values():
        default_arguments = job["Properties"].get("DefaultArguments", {})
        assert "--enable-glue-datacatalog" not in default_arguments
