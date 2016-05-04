$(document).ready(function () {
                $('#static').on('change', function () {
                  popCommit();
                });

                $('input[name=stages][value=deploy_only_prelive_webs]').on('change', function () {
                    popCommit();
                });

                // populate the short ref in commit box and disable editing
                // when doing static deploys, don't disable for prelive deploys
                function popCommit() {
                    var staticSelect = $.trim($("#static :selected").text()).toLowerCase();
                    var preliveWebsDeploy = $('input[name=stages][value=deploy_only_prelive_webs]').prop('checked')

                    if (staticSelect != 'no new static' && preliveWebsDeploy) {
                        var gitHash = staticSelect.split('-')[4].split(')')[0];
                        $("#commit").val(gitHash);
                        $("#commit").prop("disabled",false);
                    } else if (staticSelect != 'no new static' && ! preliveWebsDeploy) {
                        var gitHash = staticSelect.split('-')[4].split(')')[0];
                        $("#commit").val(gitHash);
                        $("#commit").prop("disabled",true);
                    } else {
                        $("#commit").val('origin/master');
                        $("#commit").prop("disabled",false);
                    }
                }
             });
