$(document).ready(function () {
                $('#static').on('change', function () {
                  popCommit();
                });

                // Because of the way we build forms in wtf this is brittle
                // so checking the value before making assumptions
                if ( $('#stages-6').prop('value') == 'deploy_only_prelive_webs') {
                    $('#stages-6').on('change', function () {
                        popCommit();
                    });
                }

                // populate the short ref in commit box and disable editing
                // when doing static deploys, don't disable for prelive deploys
                function popCommit() {
                    var staticSelect = $.trim($("#static :selected").text()).toLowerCase();
                    // Again being cautious in case #stages-6 changes
                    if ( $('#stages-6').prop('value') == 'deploy_only_prelive_webs') {
                      var preliveWebsDeploy = $('#stages-6').prop('checked')
                    } else {
                      var preliveWebsDeploy = false
                    }

                    if (staticSelect != 'no new static' && preliveWebsDeploy) {
                        var gitHash = staticSelect.split('-')[4].split(')')[0];
                        $("#commit").val(gitHash);
                        $("#commit").prop("disabled",false);
                        $("label[for='commit']").text("Commit (set to static commit)");
                    } else if (staticSelect != 'no new static' && ! preliveWebsDeploy) {
                        var gitHash = staticSelect.split('-')[4].split(')')[0];
                        $("#commit").val(gitHash);
                        $("#commit").prop("disabled",true);
                        $("label[for='commit']").text("LOCKED to static commit for Webs deploy");
                    } else {
                        $("#commit").val('master');
                        $("#commit").prop("disabled",false);
                        $("label[for='commit']").text("Commit");
                    }
                }
             });

