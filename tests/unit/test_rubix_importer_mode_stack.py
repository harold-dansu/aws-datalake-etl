import aws_cdk as core
import aws_cdk.assertions as assertions

from rubix_importer_mode.rubix_importer_mode_stack import RubixImporterModeStack

# example tests. To run these tests, uncomment this file along with the example
# resource in rubix_importer_mode/rubix_importer_mode_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RubixImporterModeStack(app, "rubix-importer-mode")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
