Feature: "show" command
    In order to know about a given element
    As a Geogit User
    I want to display information about it

Scenario: Try to show the description of a commit
    Given I have a repository
      And I stage 6 features
      And I run the command "commit -m TestCommit"
     When I run the command "show HEAD"
     Then the response should contain "Commit"
      And the response should contain "Author"               

  Scenario: Try to show the description of a tree
    Given I have a repository
      And I stage 6 features
      And I run the command "commit -m TestCommit"
     When I run the command "show HEAD:Points"
     Then the response should contain "TREE ID"
      And the response should contain "DEFAULT FEATURE TYPE ATTRIBUTES"
      And the response should contain "sp"
      And the response should contain "pp"
      And the response should contain "ip"
     
Scenario: Try to show the description of a feature
    Given I have a repository
      And I stage 6 features
      And I run the command "commit -m TestCommit"
     When I run the command "show HEAD:Points/Points.1"
     Then the response should contain "ATTRIBUTES"
      And the response should contain "sp"
      And the response should contain "pp"
      And the response should contain "ip"     
      
